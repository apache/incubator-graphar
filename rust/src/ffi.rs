#[cxx::bridge(namespace = "graphar")]
pub(crate) mod graphar {
    extern "C++" {
        include!("graphar_rs.h");
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(i32)]
    enum Type {
        #[cxx_name = "BOOL"]
        Bool = 0,
        #[cxx_name = "INT32"]
        Int32,
        #[cxx_name = "INT64"]
        Int64,
        #[cxx_name = "FLOAT"]
        Float,
        #[cxx_name = "DOUBLE"]
        Double,
        #[cxx_name = "STRING"]
        String,
        #[cxx_name = "LIST"]
        List,
        #[cxx_name = "DATE"]
        Date,
        #[cxx_name = "TIMESTAMP"]
        Timestamp,
        #[cxx_name = "USER_DEFINED"]
        UserDefined,
        #[cxx_name = "MAX_ID"]
        MaxId,
    }
    // C++ Enum
    unsafe extern "C++" {
        type Type;
    }

    // `DataType`
    unsafe extern "C++" {
        type DataType;

        fn Equals(&self, other: &DataType) -> bool;
        fn value_type(&self) -> &SharedPtr<DataType>;
        fn id(&self) -> Type;
        #[namespace = "graphar_rs"]
        fn to_type_name(data_type: &DataType) -> String;

        fn int32() -> &'static SharedPtr<DataType>;
        fn boolean() -> &'static SharedPtr<DataType>;
        fn int64() -> &'static SharedPtr<DataType>;
        fn float32() -> &'static SharedPtr<DataType>;
        fn float64() -> &'static SharedPtr<DataType>;
        fn string() -> &'static SharedPtr<DataType>;
        fn date() -> &'static SharedPtr<DataType>;
        fn timestamp() -> &'static SharedPtr<DataType>;
        fn list(inner: &SharedPtr<DataType>) -> SharedPtr<DataType>;
    }
}
