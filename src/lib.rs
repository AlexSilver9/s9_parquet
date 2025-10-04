use parquet::data_type::AsBytes;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use parquet::schema::types::Type;

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
pub struct Entry {
    pub timestamp_info: TimestampInfo,
    pub data: Vec<u8>,
}

pub struct ParquetWriter {
    pub writer: SerializedFileWriter<File>,
}

impl ParquetWriter {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let schema = Arc::new(parse_message_type(MESSAGE_SCHEMA)?);
        let file = File::create(path)?;
        let writer = SerializedFileWriter::new(file, schema, Default::default())?;
        let parquet_writer = ParquetWriter { writer };
        Ok(parquet_writer)
    }

    pub fn write(
        &mut self,
        entry: &Entry,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut row_group_writer = self.writer.next_row_group()?;

        // Write timestamp_millis column
        if let Some(mut col_writer) = row_group_writer.next_column()? {
            let typed_col_writer = col_writer.typed::<parquet::data_type::Int64Type>();
            typed_col_writer.write_batch(&[entry.timestamp_info.timestamp_millis], None, None)?;
            col_writer.close()?;
        }

        // Write timestamp_sec column
        if let Some(mut col_writer) = row_group_writer.next_column()? {
            let typed_col_writer = col_writer.typed::<parquet::data_type::Int64Type>();
            typed_col_writer.write_batch(&[entry.timestamp_info.timestamp_sec], None, None)?;
            col_writer.close()?;
        }

        // Write timestamp_sub_sec column
        if let Some(mut col_writer) = row_group_writer.next_column()? {
            let typed_col_writer = col_writer.typed::<parquet::data_type::Int32Type>();
            typed_col_writer.write_batch(&[entry.timestamp_info.timestamp_sub_sec], None, None)?;
            col_writer.close()?;
        }

        // Write data column
        if let Some(mut col_writer) = row_group_writer.next_column()? {
            let typed_col_writer = col_writer.typed::<parquet::data_type::ByteArrayType>();
            let byte_array = parquet::data_type::ByteArray::from(entry.data.to_vec());
            typed_col_writer.write_batch(&[byte_array], None, None)?;
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

pub struct ParquetReader {
    pub reader: SerializedFileReader<File>,
    pub schema: Type,
}

impl ParquetReader {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let schema = parse_message_type(MESSAGE_SCHEMA)?;
        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        let parquet_reader = ParquetReader { reader, schema };
        Ok(parquet_reader)
    }

    pub fn read(&self) -> Result<Vec<Entry>, Box<dyn std::error::Error>> {
        let mut entries: Vec<Entry> = Vec::new();

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

                let message: Entry = Entry {
                    timestamp_info,
                    data: bytes,
                };
                entries.push(message);
            }
        }

        Ok(entries)
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

    #[test]
    fn test_parquet_operations() {
        let test_file_path = "target/test.parquet";
        let test_timestamp_info = get_timestamp_info().unwrap();
        let test_string: String = String::from("Hello, Parquet!");
        let test_entry = Entry {
            timestamp_info: test_timestamp_info.clone(),
            data: test_string.clone().as_bytes().to_vec(),
        };
        let test_entries: Vec<Entry> = vec![test_entry.clone()];

        let mut parquet_writer = ParquetWriter::new(test_file_path).unwrap();
        parquet_writer
            .write(&test_entry)
            .unwrap();
        parquet_writer.close().unwrap();

        let parquet_reader = ParquetReader::new(test_file_path).unwrap();
        let read_entries: Vec<Entry> = parquet_reader.read().unwrap();

        assert_eq!(test_entries, read_entries);
        assert_eq!(
            test_entries.iter().nth(0).unwrap().timestamp_info,
            read_entries.iter().nth(0).unwrap().timestamp_info
        );
        assert_eq!(
            test_entries.iter().nth(0).unwrap().data,
            read_entries.iter().nth(0).unwrap().data
        );

        parquet_reader.print_metadata();
    }
}
