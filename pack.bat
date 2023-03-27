rd /s /q dist __pycache__ build
del /f /s /q *.spec mysql*.rar
rem set /p version=请输入此次打包的版本号:

rem C:\miniconda3\envs\python_code\Scripts\pyinstaller -n mysql_mig_kingbase_%version% mysql_mig_kingbase.py
C:\miniconda3\envs\python_code\Scripts\pyinstaller -F mysql_mig_pg.py
C:\miniconda3\envs\python_code\Scripts\pyinstaller -F compare_data.py

copy /y config.ini dist\
type nul > dist\custom_table.txt
pause
rem copy /y config.ini dist\mysql_mig_kingbase_%version%
rem type nul > dist\mysql_mig_kingbase_%version%\custom_table.txt

rem cd dist\mysql_mig_kingbase_%version%
rem ren mysql_mig_kingbase_%version%.exe mysql_mig_kingbase.exe
rem cd ..

rem "C:\Program Files\WinRAR\Rar.exe" a -r -s -m1 C:\PycharmProjects\python_code\mysql_mig_kingbase\mysql_mig_kingbase_%version%_win.rar mysql_mig_kingbase_%version% ^

