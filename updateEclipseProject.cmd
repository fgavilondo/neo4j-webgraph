@echo off

rem Create or update Eclipse IDE project files, e.g. after modifications to pom.xml

mvn eclipse:eclipse -DdownloadSources=true -DdownloadJavadocs=true
