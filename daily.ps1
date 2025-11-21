# ============================== 
# File: daily.ps1 
# Chạy chương trình Java
# ==============================

# Đường dẫn tới JDK 
$JAVA_HOME = "C:\Program Files\Java\jdk-17"
$JAVA = "$JAVA_HOME\bin\java.exe"

# Thư mục project target
$PROJECT = "C:\Users\Bao Tam\Desktop\HKI nam 3\Data warehouse\Project\DataWarehouse\target"

# Classpath: main classes + test classes + tất cả jar trong dependency
$CLASSPATH = "$PROJECT\classes;$PROJECT\test-classes;$PROJECT\dependency\*"

# Class chính (full qualified name)
$MAIN_CLASS = "com.example.DataWarehouse.Test"

# Chạy chương trình
& "$JAVA" -cp "$CLASSPATH" "$MAIN_CLASS"