Cấu trúc thư mục
1) .github\workflows\action.yml: file giúp chạy action (hiện tại thấy ko cần)
2) data: nơi chứa data mới về ( sẽ ko append mà tạo file mới có ngày giờ về)
3) database: gồm các file sql chứa câu lệnh cho database ( nhớ tạo file riêng cho từng database, ví dụ control.sql chứa trong folder control)
4) src\java\? : nơi chứa code chính ( các quá trình sẽ được phân chia ra từng file extract, transform, load, config là nơi xử lý cấu hình đầu vào và database là nơi để code những câu lệnh liên quan đến database)
5) src\test\java\com\example\DataWarehouse\Test: File java chính dùng để chạy toàn bộ project
6) target: chứa các file.class và dependency là chứa các thư viện cần cho code
7) config.xml : file cấu hình cho project (nhớ thay đổi khi down về máy cho hợp lý)
8) daily.ps1 : file dùng để chạy schedule dùng trong task schedule
9) Document : tài liệu cho project ( nhớ thay đổi cho đúng với bài cá nhân)
10) pom.xml: khai báo các thư viện cần dùng
11) ReadMe: dùng để đọc thông tin
