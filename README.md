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

 * no db data type fits large amounts of text, clob can't be used since it is in a group by clause, which is suboptimal
 * identifier are limited to 30 characters, using _IDENTIFIER_MAPPING table to translate to abbreviated identifiers
 * adapter doesn't support transactions yet
 * search doesn't work yet
 * unit tests require a unit test user, since there is only one test schema, running multiple tests at once breaks
 * datetime helper functions not yet implemented

### Failing Tests

 * CheckboxSetFieldTest
 * ComplexTableFieldTest
 * DataObjectDecoratorTest
 * DbDatetimeTest
 * FilesystemPublisherTest
 * HierarchyTest
 * LeftAndMainTest
 * RemoveOrphanedPagesTaskTest
 * RestfulServerTest
 * SearchContextTest
 * SearchFilterApplyRelationTest
 * SearchFormTest
 * SQLQueryTest
 * TableListFieldTest
 * TranslatableSearchFormTest
 * VersionedTest