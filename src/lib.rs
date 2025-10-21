use parquet::data_type::AsBytes;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::{SerializedFileWriter};
use parquet::schema::parser::parse_message_type;
use std::fs::File;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use arrow::array::AsArray;
use futures::StreamExt;
use futures::TryStreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::schema::types::Type;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::{ParquetMetaData};
use parquet::file::properties::{WriterProperties, WriterPropertiesPtr};

const MESSAGE_SCHEMA: &str = "\
  message schema {
    REQUIRED INT64 timestamp_millis;
    REQUIRED INT64 timestamp_sec;
    REQUIRED INT32 timestamp_sub_sec;
    REQUIRED BINARY data;
  }
";

#[derive(Debug, Clone, PartialEq)]
pub struct TimestampInfo {
    pub timestamp_millis: i64,
    pub timestamp_sec: i64,
    pub timestamp_sub_sec: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub timestamp_info: TimestampInfo,
    pub data: Vec<u8>,
}

pub struct ParquetWriter {
    pub writer: SerializedFileWriter<File>,
    pub file_path: PathBuf,
    pub max_records_per_group: usize,
}

// TOOD: Handle max limits like this:
// Err: Parquet does not support more than 32767 row groups per file (currently: 32768)

impl ParquetWriter {
    pub fn new<P: AsRef<Path>>(path: P, max_records_per_group: usize) -> anyhow::Result<Self, Box<dyn std::error::Error>> {
        let path_buf = PathBuf::from(path.as_ref());
        let schema = Arc::new(parse_message_type(MESSAGE_SCHEMA)?);
        let file = File::create(path_buf.as_path())?;

        // TODO: Make other conpressions available through configuration
        let zstd_level = ZstdLevel::try_new(5)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(zstd_level))
            .set_max_row_group_size(max_records_per_group) // TODO: Make struct with max value check
            .build();
        let props_ptr = WriterPropertiesPtr::new(props);

        let writer = SerializedFileWriter::new(file, schema, props_ptr)?;

        let parquet_writer = ParquetWriter {
            writer,
            file_path: path_buf,
            max_records_per_group
        };
        Ok(parquet_writer)
    }

    pub fn write(&mut self, records: &[Record]) -> anyhow::Result<(), Box<dyn std::error::Error>> {
        if records.is_empty() {
            return Ok(());
        }

        if records.len() > self.max_records_per_group {
            let err_str = format!("Not enough space in current row group to write records, max row group size: {}, records to write: {}",
                self.max_records_per_group, records.len());
            return Err(err_str.into());
        }

        let mut row_group_writer = self.writer.next_row_group()?;

        // Prepare data for batch writing
        let batch_size = records.len();
        let mut timestamp_millis_values = Vec::with_capacity(batch_size);
        let mut timestamp_sec_values = Vec::with_capacity(batch_size);
        let mut timestamp_sub_sec_values = Vec::with_capacity(batch_size);
        let mut data_values = Vec::with_capacity(batch_size);

        for record in records {
            timestamp_millis_values.push(record.timestamp_info.timestamp_millis);
            timestamp_sec_values.push(record.timestamp_info.timestamp_sec);
            timestamp_sub_sec_values.push(record.timestamp_info.timestamp_sub_sec);
            data_values.push(parquet::data_type::ByteArray::from(record.data.clone()));
        }

        // Write timestamp_millis column
        if let Some(mut col_writer) = row_group_writer.next_column()? {
            let typed_col_writer = col_writer.typed::<parquet::data_type::Int64Type>();
            typed_col_writer.write_batch(&timestamp_millis_values, None, None)?;
            col_writer.close()?;
        }

        // Write timestamp_sec column
        if let Some(mut col_writer) = row_group_writer.next_column()? {
            let typed_col_writer = col_writer.typed::<parquet::data_type::Int64Type>();
            typed_col_writer.write_batch(&timestamp_sec_values, None, None)?;
            col_writer.close()?;
        }

        // Write timestamp_sub_sec column
        if let Some(mut col_writer) = row_group_writer.next_column()? {
            let typed_col_writer = col_writer.typed::<parquet::data_type::Int32Type>();
            typed_col_writer.write_batch(&timestamp_sub_sec_values, None, None)?;
            col_writer.close()?;
        }

        // Write data column
        if let Some(mut col_writer) = row_group_writer.next_column()? {
            let typed_col_writer = col_writer.typed::<parquet::data_type::ByteArrayType>();
            typed_col_writer.write_batch(&data_values, None, None)?;
            col_writer.close()?;
        }

        row_group_writer.close()?;

        Ok(())
    }

    pub fn close(self) -> Result<(), Box<dyn std::error::Error>> {
        self.writer.close()?;
        Ok(())
    }
}

pub struct SyncParquetReader {
    reader: SerializedFileReader<File>,
    pub schema: Type,
    pub file_path: PathBuf,
}

impl SyncParquetReader {
    pub fn new<P: AsRef<Path>>(path: P) -> anyhow::Result<Self, Box<dyn std::error::Error>> {
        let path_buf = PathBuf::from(path.as_ref());
        let schema = parse_message_type(MESSAGE_SCHEMA)?;
        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        let parquet_reader = SyncParquetReader { reader, schema, file_path: path_buf };
        Ok(parquet_reader)
    }

    pub fn read(&self) -> anyhow::Result<Vec<Record>, Box<dyn std::error::Error>> {
        let mut records: Vec<Record> = Vec::new();

        let row_iter = self.reader.get_row_iter(Some(self.schema.clone()));
        for row_result in row_iter? {
            let row = row_result?;
            if let (
                Some(timestamp_millis),
                Some(timestamp_sec),
                Some(timestamp_sub_sec),
                Some(data_field),
            ) = (
                row.get_column_iter().nth(0),
                row.get_column_iter().nth(1),
                row.get_column_iter().nth(2),
                row.get_column_iter().nth(3),
            ) {
                let timestamp_info = TimestampInfo {
                    timestamp_millis: if let parquet::record::Field::Long(t) = &timestamp_millis.1 {
                        *t
                    } else {
                        0
                    },
                    timestamp_sec: if let parquet::record::Field::Long(t) = &timestamp_sec.1 {
                        *t
                    } else {
                        0
                    },
                    timestamp_sub_sec: if let parquet::record::Field::Int(t) = &timestamp_sub_sec.1
                    {
                        *t
                    } else {
                        0
                    },
                };

                let bytes: Vec<u8> = if let parquet::record::Field::Bytes(b) = &data_field.1 {
                    b.as_bytes().to_vec()
                } else {
                    Vec::new()
                };

                let message: Record = Record {
                    timestamp_info,
                    data: bytes,
                };
                records.push(message);
            }
        }

        Ok(records)
    }

    pub fn print_metadata(&self) {
        let metadata = self.reader.metadata();
        println!(
            "Parquet file has {} rows",
            metadata.file_metadata().num_rows()
        );
        println!("Schema: {:?}", metadata.file_metadata().schema());
    }
}

pub struct AsyncParquetReader {
    stream: ParquetRecordBatchStream<tokio::fs::File>,
    pub metadata: ParquetMetaData,
    pub file_path: PathBuf,
}

impl AsyncParquetReader {
    pub async fn new<P: AsRef<Path>>(path: P, batch_size: usize) -> anyhow::Result<Self, Box<dyn std::error::Error>> {
        let path_buf = PathBuf::from(path.as_ref());
        let file = tokio::fs::File::open(path).await?;
        let builder = ParquetRecordBatchStreamBuilder::new(file)
            .await?
            .with_batch_size(batch_size);

        let meta = builder.metadata().clone();

        let stream = builder.build()?;
        Ok(AsyncParquetReader {
            metadata: meta.deref().clone(),
            stream,
            file_path: path_buf,
        })
    }

    pub async fn read(self) -> anyhow::Result<Vec<Record>, Box<dyn std::error::Error>> {
        let results = self.stream.try_collect::<Vec<_>>().await?;
        let mut records: Vec<Record> = Vec::new();

        for batch in results {
            let num_rows = batch.num_rows();

            let timestamp_millis_array = batch.column(0).as_primitive::<arrow::datatypes::Int64Type>();
            let timestamp_sec_array = batch.column(1).as_primitive::<arrow::datatypes::Int64Type>();
            let timestamp_sub_sec_array = batch.column(2).as_primitive::<arrow::datatypes::Int32Type>();
            let data_array = batch.column(3).as_binary::<i32>();

            for i in 0..num_rows {
                let timestamp_info = TimestampInfo {
                    timestamp_millis: timestamp_millis_array.value(i),
                    timestamp_sec: timestamp_sec_array.value(i),
                    timestamp_sub_sec: timestamp_sub_sec_array.value(i),
                };
                let data = data_array.value(i).to_vec();
                let record = Record {
                    timestamp_info,
                    data,
                };
                records.push(record);
            }
        }

        Ok(records)
    }

    pub fn into_record_stream(self) -> impl futures::Stream<Item = anyhow::Result<Record, Box<dyn std::error::Error + Send + Sync>>> {
        self.stream.map(move |batch_result| {
            match batch_result {
                Ok(batch) => {
                    let num_rows = batch.num_rows();
                    let timestamp_millis_array = batch.column(0).as_primitive::<arrow::datatypes::Int64Type>();
                    let timestamp_sec_array = batch.column(1).as_primitive::<arrow::datatypes::Int64Type>();
                    let timestamp_sub_sec_array = batch.column(2).as_primitive::<arrow::datatypes::Int32Type>();
                    let data_array = batch.column(3).as_binary::<i32>();

                    let mut results = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let timestamp_info = TimestampInfo {
                            timestamp_millis: timestamp_millis_array.value(i),
                            timestamp_sec: timestamp_sec_array.value(i),
                            timestamp_sub_sec: timestamp_sub_sec_array.value(i),
                        };
                        let data = data_array.value(i).to_vec();
                        let record = Record {
                            timestamp_info,
                            data,
                        };
                        results.push(Ok(record));
                    }

                    futures::stream::iter(results)
                }
                Err(e) => {
                    let error: Box<dyn std::error::Error + Send + Sync> = Box::new(e);
                    let result = vec![Err(error)];
                    futures::stream::iter(result)
                }
            }
        }).flatten()
    }

    pub fn print_metadata(&self) {
        let metadata = &self.metadata;
        println!(
            "Parquet file has {} rows",
            metadata.file_metadata().num_rows()
        );
        println!("Schema: {:?}", metadata.file_metadata().schema());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};

    fn get_timestamp_info() -> Result<TimestampInfo, SystemTimeError> {
        let current_system_time = SystemTime::now();
        let duration_since_epoch = current_system_time.duration_since(UNIX_EPOCH)?;
        let timestamp_info = TimestampInfo {
            timestamp_millis: duration_since_epoch.as_millis() as i64,
            timestamp_sec: duration_since_epoch.as_secs() as i64,
            timestamp_sub_sec: duration_since_epoch.subsec_nanos() as i32,
        };

        //let sec = duration_since_epoch.as_secs();
        //let nanos = duration_since_epoch.subsec_nanos();
        //let ts = duration_since_epoch.as_nanos();
        Ok(timestamp_info)
    }

    fn create_test_records() -> Vec<Record> {
        let test_records = vec![
            Record {
                timestamp_info: get_timestamp_info().unwrap(),
                data: "Record 1".as_bytes().to_vec(),
            },
            Record {
                timestamp_info: get_timestamp_info().unwrap(),
                data: "Record 2".as_bytes().to_vec(),
            },
            Record {
                timestamp_info: get_timestamp_info().unwrap(),
                data: "Record 3".as_bytes().to_vec(),
            },
        ];
        test_records
    }

    #[test]
    fn test_max_max_rows_per_group_error() {
        let test_max_rows_per_group = 1;
        let test_file_path = "target/test.parquet";
        let test_records = create_test_records();

        // Write the test data first
        let mut parquet_writer = ParquetWriter::new(test_file_path, test_max_rows_per_group).unwrap();
        let result = parquet_writer.write(&test_records);
        assert_eq!(result.is_err(), true);
        parquet_writer.close().unwrap();
    }

    #[test]
    fn test_sync_parquet_operations() {
        let test_max_rows_per_group = 3;
        let test_file_path = "target/test.parquet";
        let test_records = create_test_records();

        // Write the test data first
        let mut parquet_writer = ParquetWriter::new(test_file_path, test_max_rows_per_group).unwrap();
        parquet_writer
            .write(&test_records)
            .unwrap();
        parquet_writer.close().unwrap();

        // Test sync reading
        let parquet_reader = SyncParquetReader::new(test_file_path).unwrap();
        let read_records: Vec<Record> = parquet_reader.read().unwrap();

        assert_eq!(test_records, read_records);
        for i in 0..test_records.len() {
            assert_eq!(
                test_records.iter().nth(i).unwrap().timestamp_info,
                read_records.iter().nth(i).unwrap().timestamp_info
            );
            assert_eq!(
                test_records.iter().nth(i).unwrap().data,
                read_records.iter().nth(i).unwrap().data
            );
        }

        parquet_reader.print_metadata();
    }

    #[tokio::test]
    async fn test_async_parquet_operations() {
        let test_max_rows_per_group = 3;
        let test_file_path = "target/test_async.parquet";
        let test_records = create_test_records();

        // Write the test data first
        let mut parquet_writer = ParquetWriter::new(test_file_path, test_max_rows_per_group).unwrap();
        parquet_writer.write(&test_records).unwrap();
        parquet_writer.close().unwrap();

        // Test async reading
        let parquet_reader = AsyncParquetReader::new(test_file_path, 1024).await.unwrap();
        let read_records: Vec<Record> = parquet_reader.read().await.unwrap();

        assert_eq!(test_records, read_records);
        for i in 0..test_records.len() {
            assert_eq!(
                test_records.iter().nth(i).unwrap().timestamp_info,
                read_records.iter().nth(i).unwrap().timestamp_info
            );
            assert_eq!(
                test_records.iter().nth(i).unwrap().data,
                read_records.iter().nth(i).unwrap().data
            );
        }
    }

    #[tokio::test]
    async fn test_async_stream_operations() {
        let test_max_rows_per_group = 3;
        let test_file_path = "target/test_stream.parquet";
        let test_records = create_test_records();

        // Write the test data first
        let mut parquet_writer = ParquetWriter::new(test_file_path, test_max_rows_per_group).unwrap();
        parquet_writer.write(&test_records).unwrap();
        parquet_writer.close().unwrap();

        // Test async streaming
        let async_reader = AsyncParquetReader::new(test_file_path, 1024).await.unwrap();
        let mut stream = async_reader.into_record_stream();

        let mut read_records = Vec::new();
        while let Some(result) = stream.next().await {
            match result {
                Ok(record) => read_records.push(record),
                Err(e) => panic!("Stream error: {}", e),
            }
        }

        assert_eq!(test_records, read_records);
        for i in 0..test_records.len() {
            assert_eq!(
                test_records.iter().nth(i).unwrap().timestamp_info,
                read_records.iter().nth(i).unwrap().timestamp_info
            );
            assert_eq!(
                test_records.iter().nth(i).unwrap().data,
                read_records.iter().nth(i).unwrap().data
            );
        }
    }

    #[tokio::test]
    async fn test_async_stream_multiple_records() {
        let test_max_rows_per_group = 3;
        let test_file_path = "target/test_stream_multiple.parquet";
        let test_records = create_test_records();

        // Write multiple records
        let mut parquet_writer = ParquetWriter::new(test_file_path, test_max_rows_per_group).unwrap();
        parquet_writer.write(&test_records).unwrap();
        parquet_writer.close().unwrap();

        // Test streaming multiple records
        let async_reader = AsyncParquetReader::new(test_file_path, 1024).await.unwrap();
        let stream = async_reader.into_record_stream();

        let read_records: Vec<Record> = stream
            .map(|result| result.unwrap())
            .collect()
            .await;

        assert_eq!(test_records, read_records);
        for i in 0..test_records.len() {
            assert_eq!(
                test_records.iter().nth(i).unwrap().timestamp_info,
                read_records.iter().nth(i).unwrap().timestamp_info
            );
            assert_eq!(
                test_records.iter().nth(i).unwrap().data,
                read_records.iter().nth(i).unwrap().data
            );
        }
    }
}
