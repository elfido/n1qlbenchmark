#!/bin/bash
echo Creating binaries
echo Generating Linux binary
GOOS=linux GOARCH=amd64 go build -o ./compiled/linux/n1qlb *.go
echo Generating OSX binary
GOOS=darwin GOARCH=amd64 go build -o ./compiled/osx/n1qlb *.go
echo Generating Windows binary
GOOS=windows GOARCH=amd64 go build -o ./compiled/win/n1qlb.exe *.go
echo Done!
