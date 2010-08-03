# Oracle Database Adapter

## Maintainer Contact
 * Andreas Piening <andreas (at) silverstripe (dot) com>

## Requirements
 * SilverStripe 2.4 or newer

## Installation
 1. follow the usual [module installation process](http://doc.silverstripe.org/modules#installation)
 2. add this code to your mysite/_config.php:

$databaseConfig = array(
	"type" => "OracleDatabase",
	"server" => "hostname/servicename",
	"username" => "username",
	"password" => "password",
	"database" => "SS_mysite",
);
_

## Description

## Features

## Open Issues

 * no db data type fits large amounts of text
 * identifier are limited to 30 characters
 * adapter doesn't support indexes yet
 * adapter doesn't support transactions yet
 * adapter doesn't support search yet
 * adapter doesn't support UNITTest yet
 * work in tablespace or database?
 * cms sitetree is empty