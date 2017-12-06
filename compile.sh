#!/bin/bash
echo Creating binaries
echo Generating Linux binary
GOOS=linux GOARCH=amd64 go build -o ./compiled/linux/cbmon *.go
echo Generating OSX binary
GOOS=darwin GOARCH=amd64 go build -o ./compiled/osx/cbmon *.go
echo Generating Windows binary
GOOS=windows GOARCH=amd64 go build -o ./compiled/win/cbmon.exe *.go
echo Done!
