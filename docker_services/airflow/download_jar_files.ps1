Set-Location -Path .\docker_services\airflow\jars

Write-Host "Download Path: $(Get-Location)"

function Download-Jar {
    Param (
        [string]$File,
        [string]$Url
    )

    if (Test-Path $File) {
        Write-Host "[Skipping Download] $File File Already Exists! ..."
    }
    else {
        Write-Host "[Downloading] $Url..."
        $os = [System.Environment]::OSVersion.Platform

        if ($os -eq "Win32NT") {
            Write-Host "Please download the file using the link below and place it under the ./docker_services/airflow/jars directory."
            Write-Host $Url
        }
        elseif ($os -eq "Unix") {
            if ([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::OSX)) {
                & curl -O --no-verbose $Url
            }
            else {
                & wget --no-verbose $Url
            }
        }
        else {
            Write-Host "Unsupported OS"
        }
    }
}

$deltaJarFilename = "delta-core_2.12-2.4.0.jar"
$deltaJarUrl = "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/$deltaJarFilename"

$deltaStorageJarFilename = "delta-storage-2.4.0.jar"
$deltaStorageJarUrl = "https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/$deltaStorageJarFilename"

$antLr4JarFilename = "antlr4-runtime-4.9.3.jar"
$antLr4JarUrl = "https://repo1.maven.org/maven2/io/antlr4/antlr4-runtime/4.9.3/$antLr4JarFilename"

$wildflyOpenSslJarFilename = "wildfly-openssl-1.0.7.Final.jar"
$wildflyOpenSslJarUrl = "https://repo1.maven.org/maven2/io/openssl/wildfly-openssl/1.0.7.Final/$wildflyOpenSslJarFilename"

$javaSdkJarFilename = "aws-java-sdk-bundle-1.12.392.jar"
$javaSdkJarUrl = "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.392/$javaSdkJarFilename"

$hadoopJarFilename = "hadoop-aws-3.3.1.jar"
$hadoopJarUrl = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/$hadoopJarFilename"

Download-Jar $deltaJarFilename $deltaJarUrl
Download-Jar $javaSdkJarFilename $javaSdkJarUrl
Download-Jar $hadoopJarFilename $hadoopJarUrl
Download-Jar $deltaStorageJarFilename $deltaStorageJarUrl
Download-Jar $antLr4JarFilename $antLr4JarUrl
Download-Jar $wildflyOpenSslJarFilename $wildflyOpenSslJarUrl

Set-Location -Path ..\..\..
