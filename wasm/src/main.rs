extern "C" {
    fn custom_stat(
        profile_name: *const u8,
        profile_name_length: i32,
        file_name: *const u8,
        file_name_len: i32,
    ) -> i32;
    fn custom_read(
        profile_name: *const u8,
        profile_name_length: i32,
        file_name: *const u8,
        file_name_len: i32,
        buf: *const u8,
        buflen: i32,
        readlen: &mut i32,
    ) -> i32;
    fn custom_write(
        profile_name: *const u8,
        profile_name_length: i32,
        file_name: *const u8,
        file_name_len: i32,
        buf: *const u8,
        buflen: i32,
    ) -> i32;
}

fn main() {
    unsafe {
        let file_name = "test.txt";
        let profile_name1 = "abc";
        let profile_name2 = "xyz";
        let data1 = "hello world, abc";
        let data1_len = data1.len() as i32;
        let data2 = "hello world, xyz";
        let data2_len = data2.len() as i32;
        custom_write(
            profile_name1.as_ptr(),
            profile_name1.len() as i32,
            file_name.as_ptr(),
            file_name.len() as i32,
            data1.as_ptr(),
            data1_len,
        );
        custom_write(
            profile_name2.as_ptr(),
            profile_name2.len() as i32,
            file_name.as_ptr(),
            file_name.len() as i32,
            data2.as_ptr(),
            data2_len,
        );
        let file_length1 = custom_stat(
            profile_name1.as_ptr(),
            profile_name1.len() as i32,
            file_name.as_ptr(),
            file_name.len() as i32,
        );
        let file_length2 = custom_stat(
            profile_name2.as_ptr(),
            profile_name2.len() as i32,
            file_name.as_ptr(),
            file_name.len() as i32,
        );
        println!("file_length1:{}", file_length1);
        println!("file_length2:{}", file_length2);
        let mut readlen = 0;
        let mut buf: [u8; 1024] = [0u8; 1024];
        custom_read(
            profile_name1.as_ptr(),
            profile_name1.len() as i32,
            file_name.as_ptr(),
            file_name.len() as i32,
            buf.as_ptr(),
            buf.len() as i32,
            &mut readlen,
        );
        println!("readlen:{}", readlen);
        println!("buf:{}", String::from_utf8_lossy(&buf));
        custom_read(
            profile_name2.as_ptr(),
            profile_name2.len() as i32,
            file_name.as_ptr(),
            file_name.len() as i32,
            buf.as_ptr(),
            buf.len() as i32,
            &mut readlen,
        );
        println!("readlen:{}", readlen);
        println!("buf:{}", String::from_utf8_lossy(&buf));
    }
}
