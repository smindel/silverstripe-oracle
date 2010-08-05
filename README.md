# Oracle Database Adapter

## Maintainer Contact
 * Andreas Piening <andreas (at) silverstripe (dot) com>

## Requirements
 * SilverStripe 2.4 or newer
 * Oracle Database 10g Express Edition or newer

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


## Description

## Features

## Open Issues

 * no db data type fits large amounts of text
 * identifier are limited to 30 characters
 * adapter doesn't support indexes yet
 * adapter doesn't support transactions yet
 * search/unit tests doesn't work yet
 * datetime helper functions not yet implemented
 * cms sitetree is empty due to parentid being null instead of 0
 * work in tablespace or database?