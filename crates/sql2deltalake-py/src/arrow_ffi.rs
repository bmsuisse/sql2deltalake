//! Arrow C stream / C data interface capsules in and out: batches cross the boundary without a copy.

use std::sync::Mutex;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use arrow_array::ffi_stream::{FFI_ArrowArrayStream, RecordBatchStreamReader};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader, StructArray};
use arrow_schema::SchemaRef;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

pub type Batches = Box<dyn RecordBatchReader + Send>;

fn arrow_error(err: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(format!("Invalid Arrow source: {err}"))
}

/// Opens `source` through `__arrow_c_stream__`, or `__arrow_c_array__` for one batch.
pub fn import(source: &Bound<'_, PyAny>) -> PyResult<Batches> {
    if source.hasattr("__arrow_c_stream__")? {
        let capsule = source.call_method1("__arrow_c_stream__", (source.py().None(),))?;
        let capsule = capsule.cast::<PyCapsule>()?;
        let pointer = capsule.pointer_checked(Some(c"arrow_array_stream"))?.as_ptr() as *mut FFI_ArrowArrayStream;
        // SAFETY: the capsule holds a valid stream (checked by name); moving it out and leaving
        // an empty one behind hands the release callback to us, as the C stream spec requires.
        let stream = unsafe { std::ptr::replace(pointer, FFI_ArrowArrayStream::empty()) };
        return Ok(Box::new(RecordBatchStreamReader::try_new(stream).map_err(arrow_error)?));
    }
    if source.hasattr("__arrow_c_array__")? {
        let capsules = source.call_method1("__arrow_c_array__", (source.py().None(),))?;
        let schema = capsules.get_item(0)?;
        let array = capsules.get_item(1)?;
        let schema_ptr =
            schema.cast::<PyCapsule>()?.pointer_checked(Some(c"arrow_schema"))?.as_ptr() as *const FFI_ArrowSchema;
        let array_ptr =
            array.cast::<PyCapsule>()?.pointer_checked(Some(c"arrow_array"))?.as_ptr() as *mut FFI_ArrowArray;
        // SAFETY: both capsules hold valid C data interface structs (checked by name); the
        // array is moved out so its release callback runs exactly once.
        let data = unsafe {
            let array = std::ptr::replace(array_ptr, FFI_ArrowArray::empty());
            from_ffi(array, &*schema_ptr).map_err(arrow_error)?
        };
        if !matches!(data.data_type(), arrow_schema::DataType::Struct(_)) {
            return Err(PyTypeError::new_err(format!(
                "expected a struct array or a table, got an array of {}",
                data.data_type()
            )));
        }
        let batch = RecordBatch::from(StructArray::from(data));
        let schema = batch.schema();
        return Ok(Box::new(RecordBatchIterator::new([Ok(batch)], schema)));
    }
    Err(PyTypeError::new_err("expected an Arrow stream or array (__arrow_c_stream__ / __arrow_c_array__)"))
}

/// An Arrow schema through `__arrow_c_schema__` (a pyarrow Schema, Field or table).
pub fn import_schema(source: &Bound<'_, PyAny>) -> PyResult<SchemaRef> {
    let capsule = source.call_method0("__arrow_c_schema__")?;
    let pointer =
        capsule.cast::<PyCapsule>()?.pointer_checked(Some(c"arrow_schema"))?.as_ptr() as *const FFI_ArrowSchema;
    // SAFETY: the capsule holds a valid C schema (checked by name) owned by the capsule; it is
    // only read here.
    let schema = arrow_schema::Schema::try_from(unsafe { &*pointer }).map_err(arrow_error)?;
    Ok(std::sync::Arc::new(schema))
}

/// A stream handed to Python; consumable once through `__arrow_c_stream__`
/// (`pyarrow.RecordBatchReader.from_stream`, `deltalake.write_deltalake`, ...).
#[pyclass(module = "odbc2deltalake._core", frozen)]
pub struct ArrowStream {
    schema: SchemaRef,
    reader: Mutex<Option<Batches>>,
}

impl ArrowStream {
    pub fn new(reader: Batches) -> ArrowStream {
        ArrowStream { schema: reader.schema(), reader: Mutex::new(Some(reader)) }
    }
}

#[pymethods]
impl ArrowStream {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema; // optional in the protocol; exported as is
        let reader = self
            .reader
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("the Arrow stream was already consumed"))?;
        PyCapsule::new_with_value(py, FFI_ArrowArrayStream::new(reader), c"arrow_array_stream")
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let ffi = FFI_ArrowSchema::try_from(self.schema.as_ref()).map_err(arrow_error)?;
        PyCapsule::new_with_value(py, ffi, c"arrow_schema")
    }
}
