#!/bin/bash

mvn clean package assembly:assembly
for (( i = 0; i < 5; i++ ))
do
  echo "Intento $i"
  java -jar target/basic-1.0-SNAPSHOT-jar-with-dependencies.jar
  exitcode=$?
  if [ $exitcode -ne 0 ]
  then
    echo "Error en el intento $i"
    exit
  fi
  echo "Termino del intento $i..... "
done
