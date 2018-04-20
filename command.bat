@echo off
cd src
dir *.java/s/b > source.txt
javac @source.txt -d ..\classes\
cd ..
jar.exe cvfm InvertIndexAssignment.jar classes\manifest.mf -C classes\ .
java.exe -jar InvertIndexAssignment.jar
