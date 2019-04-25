rmdir build /s /q
rmdir dist /s /q
rmdir pycompss_interactive_sc.egg-info /s /q
for /f "delims=" %%f in (files.txt) do del "%%f"
del files.txt
