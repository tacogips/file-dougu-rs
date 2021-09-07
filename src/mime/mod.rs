macro_rules! enum_str{
    (pub enum $name:ident{
        $($variant:ident = $val:expr),*,
    }) =>{
        #[derive(Clone)]
        pub enum $name {
            $($variant),*,
        }

        impl Into<&str> for $name {
            fn into(self) -> &'static str{
                match self {
                    $($name::$variant => $val), *
                }
            }
        }
    }
}

enum_str! {
    pub enum MimeType {
        OctetStream = "application/octet-stream",
        Xml = "application/xml",
        Text = "text/plain",
        Jpeg = "image/jpeg",
        Jsonl = "application/json-seq",
        Json = "application/json",
        Mp4 ="video/mp4",
    }
}
