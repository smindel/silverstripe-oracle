<?php

class OracleDatabase extends SS_Database {
	/**
	 * Connection to the DBMS.
	 * @var resource
	 */
	private $dbConn;

	/**
	 * True if we are connected to a database.
	 * @var boolean
	 */
	private $active;

	/**
	 * The name of the database.
	 * @var string
	 */
	private $database;

	private static $connection_charset = null;

	private $supportsTransactions=false;
	
	static $test_config = array();

	/**
	 * Connect to a Oracle database.
	 * @param array $parameters An map of parameters, which should include:
	 *  - server: The server, eg, localhost
	 *  - username: The username to log on with
	 *  - password: The password to log on with
	 *  - database: The database to connect to
	 *  - timezone: (optional) the timezone offset, eg: +12:00 for NZ time 
	 */
	public function __construct($parameters) {
		putenv("NLS_LANG=American_America.UTF8");
		
		$this->dbConn[$parameters['database']] = oci_connect($parameters['username'], $parameters['password'], $parameters['server'], 'UTF8');

		$this->active = true;
		$this->database = $parameters['database'];

		if(!$this->dbConn[$this->database]) {
			$this->databaseError("Couldn't connect to Oracle database");
		}

		$this->query("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'");
		$this->query("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'");
	}
	
	/**
	 * Not implemented, needed for PDO
	 */
	public function getConnect($parameters) {
		return null;
	}

	/**
	 * Returns true if this database supports collations
	 * @return boolean
	 */
	public function supportsCollations() {
		return false;
	}

	/**
	 * The DB server version.
	 * @var float
	 */
	private $oracleVersion;

	/**
	 * Get the version of the DB server.
	 * @return float
	 */
	public function getVersion() {
		if(!$this->oracleVersion) {
			$this->oracleVersion = oci_server_version($this->dbConn[$this->database]);
		}
		return $this->oracleVersion;
	}

	/**
	 * Get the database server, namely oracle.
	 * @return string
	 */
	public function getDatabaseServer() {
		return "oracle";
	}

	function mapinvalifidentifiers($match) {
		return '"' . $this->_name($match[1]) . '"';
	}

	public function query($sql, $errorLevel = E_USER_ERROR) {
		
		$pattern = '/"(\w{31,})"'.'(?=(?:(?:(?:[^\'\\\]++|\\.)*+\'){2})*+(?:[^\'\\\]++|\\.)*+$)/i';
		$sql = preg_replace_callback($pattern, 'OracleDatabase::mapinvalifidentifiers', $sql);

		if(isset($_REQUEST['previewwrite']) && in_array(strtolower(substr($sql,0,strpos($sql,' '))), array('insert','update','delete','replace'))) {
			Debug::message("Will execute: $sql");
			return;
		}

		if(isset($_REQUEST['showqueries'])) { 
			$starttime = microtime(true);
		}

		$handle = oci_parse($this->dbConn[$this->database], $sql);
		$success = oci_execute($handle);

		if(isset($_REQUEST['showqueries'])) {
			$endtime = round(microtime(true) - $starttime,4);
			if (!isset($_REQUEST['ajax'])) Debug::message("{$this->database}\n$sql\n{$endtime}ms\n", false);
			else echo "\n$sql\n{$endtime}ms\n";
		}

		if(!$handle && $errorLevel) {
			$error = oci_error();

			$this->databaseError("Couldn't run query: $sql | " . $error['message'], $errorLevel);
		}
		
		return new OracleQuery($this, $handle);
	}

	public function getGeneratedID($table) {
		return $this->query("SELECT \"{$table}_sequence\".CURRVAL AS ID FROM DUAL")->Value();
	}
	
	protected $_idmap;

	function _setupIdMapping() {
		if(!$this->query("SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME LIKE '_IDENTIFIER_MAPPING'")->value()) {
			$this->query("CREATE TABLE \"_IDENTIFIER_MAPPING\" (\"Name\" VARCHAR2(200), \"Identifier\" VARCHAR2(30))");
		}
		if(is_null($this->_idmap)) {
			$this->_idmap = array();
			if($raw = $this->query("SELECT \"Name\", \"Identifier\" FROM \"_IDENTIFIER_MAPPING\"")) foreach($raw as $match) $this->_idmap[$match['Name']] = $match['Identifier'];
		}
	}
	
	function _name($name) {
		if(is_null($this->_idmap)) $this->_setupIdMapping();
		
		if(strlen($name) > 30 && empty($this->_idmap[$name])) {
			$i = 0;
			while(empty($short)) {
				$check = substr(substr($name, 0, 26) . '_000', 0, strlen(++$i) * -1) . $i;
				if(array_search($check, $this->_idmap) === false) {
					$short = $check;
					$this->query("INSERT INTO \"_IDENTIFIER_MAPPING\" (\"Name\", \"Identifier\") VALUES ('$name', '$short')");
					$this->_idmap[$name] = $short;
				}
			}
		}
		$return = isset($this->_idmap[$name]) ? $this->_idmap[$name] : $name;

		return $return;
	}

	function _id($id) {
		if(is_null($this->_idmap)) $this->_setupIdMapping();
		$name = array_search($id, $this->_idmap);
		return $name ? $name : $id;
	}

	public function isActive() {
		return $this->active ? true : false;
	}

	public function createDatabase() {
		$this->dbConn[$this->database] = oci_connect(self::$test_config['username'], self::$test_config['password'], self::$test_config['server'], 'UTF8');
		$this->active = true;
		if(!$this->dbConn[$this->database]) {
			$this->databaseError("Couldn't connect to Oracle database");
		}
		$this->query("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'");
		$this->query("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'");

		$this->tableList = $this->fieldList = $this->indexList = $this->_idmap = null;

		$oa = new OracleAdmin(OracleDatabase::$test_config);
		$oa->dropall(false);

		return true;
	}

	/**
	 * Drop the database that this object is currently connected to.
	 * Use with caution.
	 */
	public function dropDatabase() {
		$this->dropDatabaseByName($this->database);
	}

	/**
	 * Drop the database that this object is currently connected to.
	 * Use with caution.
	 */
	public function dropDatabaseByName($dbName) {
		// $oa = new OracleAdmin(OracleDatabase::$test_config);
		// $oa->dropall(false);
	}

	/**
	 * Returns the name of the currently selected database
	 */
	public function currentDatabase() {
		return $this->database;
	}

	/**
	 * Switches to the given database.
	 * If the database doesn't exist, you should call createDatabase() after calling selectDatabase()
	 */
	public function selectDatabase($dbname) {
		$this->database = $dbname;
		$this->tableList = $this->fieldList = $this->indexList = $this->_idmap = null;
	}

	/**
	 * Returns true if the named database exists.
	 */
	public function databaseExists($name) {
		return isset($this->dbConn[$name]);
	}

	/**
	 * Returns a column 
	 */
	public function allDatabaseNames() {
		return array_keys($this->dbConn);
	}

	function clearTable($table) {
		if($table[0] != '_') $this->query("DELETE FROM \"{$table}\"");
	}
	
	function dropTable($table) {
		$this->query("DROP TRIGGER \"{$table}_trigger\"");
		$this->query("DROP SEQUENCE \"{$table}_sequence\"");
		$this->query("DROP TABLE \"$table\"");
	}
	
	/**
	 * Create a new table.
	 * @param $tableName The name of the table
	 * @param $fields A map of field names to field types
	 * @param $indexes A map of indexes
	 * @param $options An map of additional options.  The available keys are as follows:
	 *   - 'MSSQLDatabase'/'MySQLDatabase'/'PostgreSQLDatabase' - database-specific options such as "engine" for MySQL.
	 *   - 'temporary' - If true, then a temporary table will be created
	 * @return The table name generated.  This may be different from the table name, for example with temporary tables.
	 */
	public function createTable($table, $fields = null, $indexes = null, $options = null, $advancedOptions = null) {
		$sequence = $table . '_sequence';
		$trigger = $table . '_trigger';
		
		$fieldSchemas = $indexSchemas = "";

		if(!isset($fields['ID'])) $fields['ID'] = "NUMBER PRIMARY KEY";
		if($fields) foreach($fields as $k => $v) $fieldSchemas[] = "\"$k\" $v";
		if($indexes) foreach($indexes as $k => $v) $indexSchemas .= $this->getIndexSqlDefinition($k, $v) . ",\n";

		// Switch to "CREATE TEMPORARY TABLE" for temporary tables
		$temporary = empty($options['temporary']) ? array('', '') : array('GLOBAL TEMPORARY', '');

		$lb = ' ';
		$this->query("CREATE {$temporary[0]} TABLE \"$table\" (\n\t" . implode(",\n\t", $fieldSchemas) . ",\nPRIMARY KEY (ID))");
		$this->query("CREATE SEQUENCE \"$sequence\" START WITH 1 INCREMENT BY 1");
		$this->query("CREATE OR REPLACE TRIGGER \"$trigger\"{$lb}BEFORE INSERT ON \"{$table}\"{$lb}FOR EACH ROW{$lb}DECLARE{$lb}max_id NUMBER;{$lb}cur_seq NUMBER;{$lb}BEGIN{$lb}IF :new.\"ID\" IS NULL THEN{$lb}SELECT \"$sequence\".nextval INTO :new.\"ID\" FROM DUAL;{$lb}ELSE{$lb}SELECT GREATEST(MAX(\"ID\"), :new.\"ID\") INTO max_id FROM \"$table\";{$lb}SELECT \"$sequence\".nextval INTO cur_seq FROM DUAL;{$lb}WHILE cur_seq < max_id{$lb}LOOP{$lb}SELECT \"$sequence\".nextval INTO cur_seq FROM DUAL;{$lb}END LOOP;{$lb}END IF;{$lb}END;{$lb}");

		if($indexes) {
			foreach($indexes as $indexName => $indexDetails) {
				if(is_array($indexDetails)) {
					$type = strtoupper($indexDetails['type']);
					preg_match_all('/(\w+)/', $indexDetails['value'], $columns);
					$columns = $columns[1];
				} else {
					$type = '';
					preg_match_all('/(\w+)/', $indexDetails, $columns);
					$columns = $columns[1];
				}
				$this->query("CREATE $type INDEX \"{$table}_{$indexName}\" ON \"{$table}\" (\n\t\"" . implode("\",\n\t\"", $columns) . "\")");
			}
		}
		
		return $table;
	}

	/**
	 * Alter a table's schema.
	 * @param $table The name of the table to alter
	 * @param $newFields New fields, a map of field name => field schema
	 * @param $newIndexes New indexes, a map of index name => index type
	 * @param $alteredFields Updated fields, a map of field name => field schema
	 * @param $alteredIndexes Updated indexes, a map of index name => index type
	 * @param $alteredOptions
	 */
	public function alterTable($tableName, $newFields = null, $newIndexes = null, $alteredFields = null, $alteredIndexes = null, $alteredOptions = null, $advancedOptions = null) {
		
		$fieldSchemas = $indexSchemas = "";
		$alterList = array();

		if($newFields) foreach($newFields as $k => $v) { 
			if(preg_match('/\sPRIMARY KEY$/', $v)) {
				$v = substr($v, 0, -12);
				$pk[] = $k;
			}
			$newFieldList[] = "\"$k\" $v";
		}
		if($alteredFields) foreach($alteredFields as $k => $v) {
			if(preg_match('/\sPRIMARY KEY$/', $v)) {
				$v = substr($v, 0, -12);
				$pk[] = $k;
			}
			$alterFieldList[] = "\"$k\" $v";
		}

		if($newIndexes) {
			Debug::dump('skip adding index', $newIndexes);
			//foreach($newIndexes as $k => $v) $alterList[] .= "ADD " . $this->getIndexSqlDefinition($k, $v);
		}
		if($alteredIndexes) {
			Debug::dump('skip changing index', $alteredIndexes);
			// foreach($alteredIndexes as $k => $v) {
			// 	$alterList[] .= "DROP INDEX \"$k\"";
			// 	$alterList[] .= "ADD ". $this->getIndexSqlDefinition($k, $v);
			// }
		}

		$alterations = '';
 		if(!empty($newFieldList)) $alterations .= ' ADD(' . implode(",\n", $newFieldList) . ')';
 		if(!empty($alterFieldList)) $alterations .= ' MODIFY(' . implode(",\n", $alterFieldList) . ')';
// 		if(!empty($pk)) $alterations .= ' ADD CONSTRAINT "' . $tableName . '_pk" PRIMARY KEY ("' . implode('","', $pk) . '")';
		if(!empty($alterations)) $this->query("ALTER TABLE \"$tableName\" $alterations");

		if($alteredOptions && isset($alteredOptions[get_class($this)])) {
			$this->query(sprintf("ALTER TABLE \"%s\" %s", $tableName, $alteredOptions[get_class($this)]));
			DB::alteration_message(
				sprintf("Table %s options changed: %s", $tableName, $alteredOptions[get_class($this)]),
				"changed"
			);
		}
	}

	public function renameTable($oldTableName, $newTableName) {
		$this->query("ALTER TABLE \"$oldTableName\" RENAME \"$newTableName\"");
	}



	/**
	 * Checks a table's integrity and repairs it if necessary.
	 * @var string $tableName The name of the table.
	 * @return boolean Return true if the table has integrity after the method is complete.
	 */
	public function checkAndRepairTable($tableName) {
		if(!$this->runTableCheckCommand("CHECK TABLE \"$tableName\"")) {
			if($this->runTableCheckCommand("CHECK TABLE \"".strtolower($tableName)."\"")){
				DB::alteration_message("Table $tableName: renamed from lowercase","repaired");
				return $this->renameTable(strtolower($tableName),$tableName);
			}

			DB::alteration_message("Table $tableName: repaired","repaired");
			return $this->runTableCheckCommand("REPAIR TABLE \"$tableName\" USE_FRM");
		} else {
			return true;
		}
	}

	/**
	 * Helper function used by checkAndRepairTable.
	 * @param string $sql Query to run.
	 * @return boolean Returns if the query returns a successful result.
	 */
	protected function runTableCheckCommand($sql) {
		return true;
		$testResults = $this->query($sql);
		foreach($testResults as $testRecord) {
			if(strtolower($testRecord['Msg_text']) != 'ok') {
				return false;
			}
		}
		return true;
	}

	public function createField($tableName, $fieldName, $fieldSpec) {
		$this->query("ALTER TABLE \"$tableName\" ADD \"$fieldName\" $fieldSpec");
	}

	/**
	 * Change the database type of the given field.
	 * @param string $tableName The name of the tbale the field is in.
	 * @param string $fieldName The name of the field to change.
	 * @param string $fieldSpec The new field specification
	 */
	public function alterField($tableName, $fieldName, $fieldSpec) {
		$this->query("ALTER TABLE \"$tableName\" CHANGE \"$fieldName\" \"$fieldName\" $fieldSpec");
	}

	/**
	 * Change the database column name of the given field.
	 * 
	 * @param string $tableName The name of the tbale the field is in.
	 * @param string $oldName The name of the field to change.
	 * @param string $newName The new name of the field
	 */
	public function renameField($tableName, $oldName, $newName) {
		$fieldList = $this->fieldList($tableName);
		if(array_key_exists($oldName, $fieldList)) {
			$this->query("ALTER TABLE \"$tableName\" RENAME COLUMN \"$oldName\" TO \"$newName\"");
		}
	}

	private static $_cache_collation_info = array();

	public function fieldList($table) {
		
		$table = $this->_name($table);
		$fields = DB::query("SELECT * FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '$table' ORDER BY COLUMN_ID");

		foreach($fields as $field) {

			$fieldSpec = $field['DATA_TYPE'];

			if(preg_match('/^TIMESTAMP/i', $field['DATA_TYPE'])) {
				$fieldSpec = "TIMESTAMP";
			} else if(preg_match('/^DATE/i', $field['DATA_TYPE'])) {
				$fieldSpec = "DATE";
			} else if(!empty($field['DATA_PRECISION'])) {
				$fieldSpec .= empty($field['DATA_SCALE']) ? "({$field['DATA_PRECISION']})" : "({$field['DATA_PRECISION']},{$field['DATA_SCALE']})";
			} else if($field['DATA_LENGTH']) {
				$fieldSpec .= "({$field['DATA_LENGTH']})";
			}

			if($field['DATA_DEFAULT'] || $field['DATA_DEFAULT'] === "0" || $field['DATA_DEFAULT'] === 0) {
				$fieldSpec .= " DEFAULT " . trim($field['DATA_DEFAULT']);
			}

			if(!$field['NULLABLE'] || $field['NULLABLE'] == 'N') {
				$fieldSpec .= ' NOT NULL';
			}

			$fieldList[$this->_name($field['COLUMN_NAME'])] = $fieldSpec;
		}
		return $fieldList;
	}

	/**
	 * Create an index on a table.
	 * 
	 * @param string $tableName The name of the table.
	 * @param string $indexName The name of the index.
	 * @param string $indexSpec The specification of the index, see {@link SS_Database::requireIndex()} for more details.
	 */
	public function createIndex($tableName, $indexName, $indexSpec) {
		$this->query("ALTER TABLE \"$tableName\" ADD " . $this->getIndexSqlDefinition($indexName, $indexSpec));
	}

	/**
	 * This takes the index spec which has been provided by a class (ie static $indexes = blah blah)
	 * and turns it into a proper string.
	 * Some indexes may be arrays, such as fulltext and unique indexes, and this allows database-specific
	 * arrays to be created. See {@link requireTable()} for details on the index format.
	 * 
	 * @param string|array $indexSpec
	 * @return string PL/SQL compatible ALTER TABLE syntax
	 */
	public function convertIndexSpec($indexSpec){
		if(is_array($indexSpec)){
			$indexSpec['value'] = str_replace(' ', '', $indexSpec['value']);
			//Here we create a db-specific version of whatever index we need to create.
			switch($indexSpec['type']){
				case 'fulltext':
					$indexSpec='fulltext (' . $indexSpec['value'] . ')';
					break;
				case 'unique':
					$indexSpec='unique (' . $indexSpec['value'] . ')';
					break;
				case 'btree':
					$indexSpec='using btree (' . $indexSpec['value'] . ')';
					break;
				case 'hash':
					$indexSpec='using hash (' . $indexSpec['value'] . ')';
					break;
			}
		} else {
			$indexSpec = str_replace(' ', '', $indexSpec);
		}
		return $indexSpec;
	}

	/**
	 * @param string $indexName
	 * @param string|array $indexSpec See {@link requireTable()} for details
	 * @return string PL/SQL compatible ALTER TABLE syntax
	 */
	protected function getIndexSqlDefinition($indexName, $indexSpec=null) {

		$indexSpec=$this->convertIndexSpec($indexSpec);

		$indexSpec = trim($indexSpec);
		if($indexSpec[0] != '(') list($indexType, $indexFields) = explode(' ',$indexSpec,2);
	    else $indexFields = $indexSpec;

	    if(!isset($indexType))
			$indexType = "index";

		if($indexType=='using')
			return "index \"$indexName\" using $indexFields";  
		else {
			return "$indexType \"$indexName\" $indexFields";
		}

	}

	function getDbSqlDefinition($tableName, $indexName, $indexSpec){
		return $indexName;
	}

	/**
	 * Alter an index on a table.
	 * @param string $tableName The name of the table.
	 * @param string $indexName The name of the index.
	 * @param string $indexSpec The specification of the index, see {@link SS_Database::requireIndex()} for more details.
	 */
	public function alterIndex($tableName, $indexName, $indexSpec) {

		$indexSpec=$this->convertIndexSpec($indexSpec);

		$indexSpec = trim($indexSpec);
	    if($indexSpec[0] != '(') {
	    	list($indexType, $indexFields) = explode(' ',$indexSpec,2);
	    } else {
	    	$indexFields = $indexSpec;
	    }

	    if(!$indexType) {
	    	$indexType = "index";
	    }

		$this->query("ALTER TABLE \"$tableName\" DROP INDEX \"$indexName\"");
		$this->query("ALTER TABLE \"$tableName\" ADD $indexType \"$indexName\" $indexFields");
	}

	/**
	 * Return the list of indexes in a table.
	 * @param string $table The table name.
	 * @return array
	 */
	public function indexList($table) {

		$table = $this->_name($table);

		$indexes = DB::query("SELECT * FROM USER_INDEXES WHERE TABLE_NAME = '{$table}'");
		
		$indexList = array();
		foreach($indexes as $index) {
			$cols = array();
			foreach(DB::query("SELECT * FROM USER_IND_COLUMNS WHERE INDEX_NAME = '{$index['INDEX_NAME']}'") as $col) {
				$cols[$col['COLUMN_POSITION']] = $col['COLUMN_NAME'];
			}
			ksort($cols);
			$name = implode('_',$cols);
			$spec = implode(',',$cols);
			$indexList[$name] = $index['UNIQUENESS'] == 'UNIQUE' ? 'unique ' : '';
			$indexList[$name] .= "($spec)";
		}

		return $indexList;
	}

	/**
	 * Returns a list of all the tables in the database.
	 * @return array
	 */
	public function tableList() {
		$tables = array();
		foreach($this->query("SELECT TABLE_NAME FROM USER_TABLES") as $record) {
			$table = reset($record);
			$tables[strtolower($table)] = $this->_id($table);
		}
		return $tables;
	}

	/**
	 * Return the number of rows affected by the previous operation.
	 * @return int 
	 */
	public function affectedRows() {
		trigger_error('not yet implemented');
	}

	function databaseError($msg, $errorLevel = E_USER_ERROR) {
		// try to extract and format query
		if(preg_match('/Couldn\'t run query: ([^\|]*)\|\s*(.*)/', $msg, $matches)) {
			$formatter = new SQLFormatter();
			$msg = "Couldn't run query: \n" . $formatter->formatPlain($matches[1]) . "\n\n" . $matches[2];
		}

		user_error($msg, $errorLevel);
	}

	/**
	 * Return a boolean type-formatted string
	 * 
	 * @param array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function boolean($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'tinyint', 'precision'=>1, 'sign'=>'unsigned', 'null'=>'not null', 'default'=>$this->default);
		//DB::requireField($this->tableName, $this->name, "tinyint(1) unsigned not null default '{$this->defaultVal}'");
		return "CHAR(1) DEFAULT 0";
	}

	/**
	 * Return a date type-formatted string
	 * 
	 * @param array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function date($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'date');
		//DB::requireField($this->tableName, $this->name, "date");

		return 'DATE';
	}

	/**
	 * Return a decimal type-formatted string
	 * 
	 * @param array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function decimal($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'decimal', 'precision'=>"$this->wholeSize,$this->decimalSize");
		//DB::requireField($this->tableName, $this->name, "decimal($this->wholeSize,$this->decimalSize)");

		// Avoid empty strings being put in the db
		if($values['precision'] == '') {
			$precision = '11,1';
		} else {
			$precision = $values['precision'];
		}

		$defaultValue = '';
		if(isset($values['default'])) {
			$defaultValue = ' DEFAULT ' . (float)$values['default'];
		}

		return "NUMBER($precision)$defaultValue NOT NULL";
	}

	/**
	 * Return a enum type-formatted string
	 * 
	 * @param array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	protected $enum_map = array();
	
	public function enum($values){
		$tablefield = $values['table'] . '.' . $values['name'];
		if(!$this->hasTable('_ORACLE_ENUMS')) $this->query("CREATE TABLE \"_ORACLE_ENUMS\" (\"TableColumn\" VARCHAR2(200), \"EnumList\" VARCHAR2(2000))");
		if(empty($this->enum_map[$tablefield]) || $this->enum_map[$tablefield] != implode(',', $values['enums'])) {
			if($this->query("SELECT COUNT(*) FROM \"_ORACLE_ENUMS\" WHERE \"TableColumn\" = '{$tablefield}'")->Value()) {
				$this->query("UPDATE \"_ORACLE_ENUMS\" SET \"EnumList\" = '" . implode(',', $values['enums']) . "' WHERE \"TableColumn\" = '{$tablefield}'");
			} else {
				$this->query("INSERT INTO \"_ORACLE_ENUMS\" (\"TableColumn\", \"EnumList\") VALUES ('{$tablefield}', '" . implode(',', $values['enums']) . "')");
			}
			$this->enum_map[$tablefield] = implode(',', $values['enums']);
		}
		return "VARCHAR2(2000) DEFAULT '{$values['default']}'";
	}

	/**
	 * Return a set type-formatted string
	 * 
	 * @param array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function set($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'enum', 'enums'=>$this->enum, 'character set'=>'utf8', 'collate'=> 'utf8_general_ci', 'default'=>$this->default);
		//DB::requireField($this->tableName, $this->name, "enum('" . implode("','", $this->enum) . "') character set utf8 collate utf8_general_ci default '{$this->default}'");
		$default = empty($values['default']) ? '' : " default '$values[default]'";
		return 'set(\'' . implode('\',\'', $values['enums']) . '\') ' . $default;
	}

	/**
	 * Return a float type-formatted string
	 * 
	 * @param array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function float($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'float');
		//DB::requireField($this->tableName, $this->name, "float");

		return 'FLOAT not null default ' . $values['default'];
	}

	/**
	 * Return a int type-formatted string
	 * 
	 * @param array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function int($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'int', 'precision'=>11, 'null'=>'not null', 'default'=>(int)$this->default);
		//DB::requireField($this->tableName, $this->name, "int(11) not null default '{$this->defaultVal}'");

//		return 'NUMBER(11) NOT NULL';
		return 'NUMBER(11) DEFAULT ' . (int)$values['default'] . ' NOT NULL';
	}

	/**
	 * Return a datetime type-formatted string
	 * 
	 * @param array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function ss_datetime($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'datetime');
		//DB::requireField($this->tableName, $this->name, $values);

		return 'TIMESTAMP';
	}

	/**
	 * Return a text type-formatted string
	 * 
	 * @param array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function text($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'mediumtext', 'character set'=>'utf8', 'collate'=>'utf8_general_ci');
		//DB::requireField($this->tableName, $this->name, "mediumtext character set utf8 collate utf8_general_ci");

		return 'VARCHAR2(4000)';
		return 'CLOB'; // does not work with DataObject::getManyManyComponentsQuery() because it is grouping by CLOBS
	}

	/**
	 * Return a time type-formatted string
	 * 
	 * @param array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function time($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'time');
		//DB::requireField($this->tableName, $this->name, "time");

		return 'TIMESTAMP';
	}

	/**
	 * Return a varchar type-formatted string
	 * 
	 * @param array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function varchar($values){
		//For reference, this is what typically gets passed to this function:
		//$parts=Array('datatype'=>'varchar', 'precision'=>$this->size, 'character set'=>'utf8', 'collate'=>'utf8_general_ci');
		//DB::requireField($this->tableName, $this->name, "varchar($this->size) character set utf8 collate utf8_general_ci");

		return 'VARCHAR2(' . $values['precision'] . ')';
	}

	public function year($values){
		return 'NUMBER';
	}
	/**
	 * This returns the column which is the primary key for each table
	 * In Postgres, it is a SERIAL8, which is the equivalent of an auto_increment
	 *
	 * @return string
	 */
	function IdColumn(){
		return 'NUMBER(11) NOT NULL';
	}

	/**
	 * Returns the SQL command to get all the tables in this database
	 */
	function allTablesSQL() {
		if(is_null($this->_idmap)) $this->_setupIdMapping();
		return "SELECT CASE WHEN \"Name\" IS NOT NULL THEN \"Name\" ELSE TABLE_NAME END AS \"Tablename\" FROM USER_TABLES LEFT JOIN \"_IDENTIFIER_MAPPING\" ON USER_TABLES.TABLE_NAME = \"_IDENTIFIER_MAPPING\".\"Identifier\"";
	}

	/**
	 * Returns true if the given table is exists in the current database 
	 * NOTE: Experimental; introduced for db-abstraction and may changed before 2.4 is released.
	 */
	public function hasTable($table) {
		$SQL_table = Convert::raw2sql($table);
		$SQL_table = $this->_name($SQL_table);
		return (bool)($this->query("SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME LIKE '$SQL_table'")->value());
	}

	/**
	 * Returns the values of the given enum field
	 * NOTE: Experimental; introduced for db-abstraction and may changed before 2.4 is released.
	 */
	public function enumValuesForField($tableName, $fieldName) {
		$classnameinfo = DB::query("SELECT \"EnumList\" FROM \"_ORACLE_ENUMS\" WHERE \"TableColumn\" = '{$tableName}.{$fieldName}'")->first();
		$output = array();
		if($classnameinfo) {
			$output = explode(',', $classnameinfo['EnumList']);
		}
		return $output;
	}

	/**
	 * The core search engine, used by this class and its subclasses to do fun stuff.
	 * Searches both SiteTree and File.
	 * 
	 * @param string $keywords Keywords as a string.
	 */
	public function searchEngine($classesToSearch, $keywords, $start, $pageLength, $sortBy = "Relevance DESC", $extraFilter = "", $booleanSearch = false, $alternativeFileFilter = "", $invertedMatch = false) {
		$fileFilter = '';	 	
	 	$keywords = Convert::raw2sql($keywords);
		$htmlEntityKeywords = htmlentities($keywords,ENT_NOQUOTES);

		$extraFilters = array('SiteTree' => '', 'File' => '');

	 	if($booleanSearch) $boolean = "IN BOOLEAN MODE";

	 	if($extraFilter) {
	 		$extraFilters['SiteTree'] = " AND $extraFilter";

	 		if($alternativeFileFilter) $extraFilters['File'] = " AND $alternativeFileFilter";
	 		else $extraFilters['File'] = $extraFilters['SiteTree'];
	 	}

		// Always ensure that only pages with ShowInSearch = 1 can be searched
		$extraFilters['SiteTree'] .= " AND ShowInSearch <> 0";

		$limit = $start . ", " . (int) $pageLength;

		$notMatch = $invertedMatch ? "NOT " : "";
		if($keywords) {
			$match['SiteTree'] = "
				MATCH (Title, MenuTitle, Content, MetaTitle, MetaDescription, MetaKeywords) AGAINST ('$keywords' $boolean)
				+ MATCH (Title, MenuTitle, Content, MetaTitle, MetaDescription, MetaKeywords) AGAINST ('$htmlEntityKeywords' $boolean)
			";
			$match['File'] = "MATCH (Filename, Title, Content) AGAINST ('$keywords' $boolean) AND ClassName = 'File'";

			// We make the relevance search by converting a boolean mode search into a normal one
			$relevanceKeywords = str_replace(array('*','+','-'),'',$keywords);
			$htmlEntityRelevanceKeywords = str_replace(array('*','+','-'),'',$htmlEntityKeywords);
			$relevance['SiteTree'] = "MATCH (Title, MenuTitle, Content, MetaTitle, MetaDescription, MetaKeywords) AGAINST ('$relevanceKeywords') + MATCH (Title, MenuTitle, Content, MetaTitle, MetaDescription, MetaKeywords) AGAINST ('$htmlEntityRelevanceKeywords')";
			$relevance['File'] = "MATCH (Filename, Title, Content) AGAINST ('$relevanceKeywords')";
		} else {
			$relevance['SiteTree'] = $relevance['File'] = 1;
			$match['SiteTree'] = $match['File'] = "1 = 1";
		}

		// Generate initial queries and base table names
		$baseClasses = array('SiteTree' => '', 'File' => '');
		foreach($classesToSearch as $class) {
			$queries[$class] = singleton($class)->extendedSQL($notMatch . $match[$class] . $extraFilters[$class], "");
			$baseClasses[$class] = reset($queries[$class]->from);
		}

		// Make column selection lists
		$select = array(
			'SiteTree' => array("ClassName","$baseClasses[SiteTree].ID","ParentID","Title","MenuTitle","URLSegment","Content","LastEdited","Created","_utf8'' AS Filename", "_utf8'' AS Name", "$relevance[SiteTree] AS Relevance", "CanViewType"),
			'File' => array("ClassName","$baseClasses[File].ID","_utf8'' AS ParentID","Title","_utf8'' AS MenuTitle","_utf8'' AS URLSegment","Content","LastEdited","Created","Filename","Name","$relevance[File] AS Relevance","NULL AS CanViewType"),
		);

		// Process queries
		foreach($classesToSearch as $class) {
			// There's no need to do all that joining
			$queries[$class]->from = array(str_replace('`','',$baseClasses[$class]) => $baseClasses[$class]);
			$queries[$class]->select = $select[$class];
			$queries[$class]->orderby = null;
		}

		// Combine queries
		$querySQLs = array();
		$totalCount = 0;
		foreach($queries as $query) {
			$querySQLs[] = $query->sql();
			$totalCount += $query->unlimitedRowCount();
		}
		$fullQuery = implode(" UNION ", $querySQLs) . " ORDER BY $sortBy LIMIT $limit";

		// Get records
		$records = DB::query($fullQuery);

		foreach($records as $record)
			$objects[] = new $record['ClassName']($record);

		if(isset($objects)) $doSet = new DataObjectSet($objects);
		else $doSet = new DataObjectSet();

		$doSet->setPageLimits($start, $pageLength, $totalCount);
		return $doSet;
	}

	function now(){
		return "SYSDATE";
	}

	/*
	 * Returns the database-specific version of the random() function
	 */
	function random(){
		return 'DBMS_RANDOM.VALUE';
	}

	/*
	 * This is a lookup table for data types.
	 * For instance, Postgres uses 'INT', while MySQL uses 'UNSIGNED'
	 * So this is a DB-specific list of equivilents.
	 */
	function dbDataType($type){
		$values=Array(
			'unsigned integer'=>'UNSIGNED'
		);

		if(isset($values[$type]))
			return $values[$type];
		else return '';
	}

	public function sqlQueryToString(SQLQuery $sqlQuery) {
		if (!$sqlQuery->from) return '';
		$distinct = $sqlQuery->distinct ? "DISTINCT " : "";
		if($sqlQuery->delete) {
			$text = "DELETE ";
		} else if($sqlQuery->select) {
			$selects = array();
			foreach($sqlQuery->select as $select) {
				if(preg_match('/"(\w+)"$/i', $select, $matches)) {
					$selects['"' . $matches[1] . '"'] =  $select;
				} else {
					$selects[$select] =  $select;
				}
			}
			$text = "SELECT $distinct" . implode(", ", $selects);
		}
		$text .= " FROM " . implode(" ", $sqlQuery->from);

		if($sqlQuery->where) $text .= " WHERE (" . $sqlQuery->getFilter(). ")";
		if($sqlQuery->groupby) $text .= " GROUP BY " . implode(", ", $sqlQuery->groupby);
		if($sqlQuery->having) $text .= " HAVING ( " . implode(" ) AND ( ", $sqlQuery->having) . " )";
		if($sqlQuery->orderby) {
			$text .= " ORDER BY ";
			if(preg_match('/^([a-z0-9_]+)(\s+ASC|\s+DESC)?$/i', $sqlQuery->orderby, $matches)) $text .= '"' . $matches[1] . '"' . (isset($matches[2]) ? $matches[2] : '');
			else $text .= $sqlQuery->orderby;
		}

		if($sqlQuery->limit) {
			$limit = $sqlQuery->limit;
			// Pass limit as array or SQL string value

			// if(is_string($limit) && preg_match('/(\d+)\s*,\s*(\d+)/', trim($limit), $matches)) {
			// 	$limit = array(
			// 		'start' => $matches[1],
			// 		'limit' => $matches[2],
			// 	);
			// }

			if(is_array($limit)) {
				if(!array_key_exists('limit',$limit)) user_error('SQLQuery::limit(): Wrong format for $limit', E_USER_ERROR);

				if(isset($limit['start']) && is_numeric($limit['start']) && isset($limit['limit']) && is_numeric($limit['limit'])) {
					$combinedLimit = "ROWNUM BETWEEN " . ($limit['start'] + 1) . " AND " . ($limit['start'] + $limit['limit']);
				} elseif(isset($limit['limit']) && is_numeric($limit['limit'])) {
					$combinedLimit = "ROWNUM <= " . (int)$limit['limit'];
				} else {
					$combinedLimit = false;
				}
				if(!empty($combinedLimit)) $text = "SELECT " . implode(", ", array_keys($selects)) . " FROM ($text) WHERE $combinedLimit";

			} else {
				$text = "SELECT " . implode(", ", array_keys($selects)) . " FROM ($text) WHERE ROWNUM <= " . $sqlQuery->limit;
			}
		}
		
		return $text;
	}
	
	/*
	 * This will return text which has been escaped in a database-friendly manner
	 * Using PHP's addslashes method won't work in MSSQL
	 */
	function addslashes($value){
		return str_replace("'", "''", $value);
	}

	/*
	 * This changes the index name depending on database requirements.
	 */
	function modifyIndex($index){
		return $index;
	}

	/**
	 * Returns a SQL fragment for querying a fulltext search index
	 * @param $fields array The list of field names to search on
	 * @param $keywords string The search query
	 * @param $booleanSearch A MySQL-specific flag to switch to boolean search
	 */
	function fullTextSearchSQL($fields, $keywords, $booleanSearch = false) {
		$boolean = $booleanSearch ? "IN BOOLEAN MODE" : "";
		$fieldNames = '"' . implode('", "', $fields) . '"';

	 	$SQL_keywords = Convert::raw2sql($keywords);
		$SQL_htmlEntityKeywords = Convert::raw2sql(htmlentities($keywords));

		return "(MATCH ($fieldNames) AGAINST ('$SQL_keywords' $boolean) + MATCH ($fieldNames) AGAINST ('$SQL_htmlEntityKeywords' $boolean))";
	}

	/*
	 * Does this database support transactions?
	 */
	public function supportsTransactions(){
		return $this->supportsTransactions;
	}

	/*
	 * This is a quick lookup to discover if the database supports particular extensions
	 */
	public function supportsExtensions($extensions=Array('partitions', 'tablespaces', 'clustering')){
		if(isset($extensions['partitions']))
			return false;
		elseif(isset($extensions['tablespaces']))
			return false;
		elseif(isset($extensions['clustering']))
			return false;
		else
			return false;
	}

	/*
	 * Start a prepared transaction
	 * See http://developer.postgresql.org/pgdocs/postgres/sql-set-transaction.html for details on transaction isolation options
	 */
	public function startTransaction($transaction_mode=false, $session_characteristics=false){
		//Transactions not set up for oci yet
	}

	/*
	 * Create a savepoint that you can jump back to if you encounter problems
	 */
	public function transactionSavepoint($savepoint){
		//Transactions not set up for oci yet
	}

	/*
	 * Rollback or revert to a savepoint if your queries encounter problems
	 * If you encounter a problem at any point during a transaction, you may
	 * need to rollback that particular query, or return to a savepoint
	 */
	public function transactionRollback($savepoint=false){
		//Transactions not set up for oci yet
	}

	/*
	 * Commit everything inside this transaction so far
	 */
	public function endTransaction(){
		//Transactions not set up for oci yet
	}

	/**
	 * Function to return an SQL datetime expression that can be used with oci
	 * used for querying a datetime in a certain format
	 * @param string $date to be formated, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @param string $format to be used, supported specifiers:
	 * %Y = Year (four digits)
	 * %m = Month (01..12)
	 * %d = Day (01..31)
	 * %H = Hour (00..23)
	 * %i = Minutes (00..59)
	 * %s = Seconds (00..59)
	 * %U = unix timestamp, can only be used on it's own
	 * @return string SQL datetime expression to query for a formatted datetime
	 */
	function formattedDatetimeClause($date, $format) {

		preg_match_all('/%(.)/', $format, $matches);
		foreach($matches[1] as $match) if(array_search($match, array('Y','m','d','H','i','s','U')) === false) user_error('formattedDatetimeClause(): unsupported format character %' . $match, E_USER_WARNING);

		if(preg_match('/^now$/i', $date)) {
			$date = "NOW()";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date)) {
			$date = "'$date'";
		}

		if($format == '%U') return "UNIX_TIMESTAMP($date)";

		return "DATE_FORMAT($date, '$format')";

	}

	/**
	 * Function to return an SQL datetime expression that can be used with oci
	 * used for querying a datetime addition
	 * @param string $date, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @param string $interval to be added, use the format [sign][integer] [qualifier], e.g. -1 Day, +15 minutes, +1 YEAR
	 * supported qualifiers:
	 * - years
	 * - months
	 * - days
	 * - hours
	 * - minutes
	 * - seconds
	 * This includes the singular forms as well
	 * @return string SQL datetime expression to query for a datetime (YYYY-MM-DD hh:mm:ss) which is the result of the addition
	 */
	function datetimeIntervalClause($date, $interval) {

		$interval = preg_replace('/(year|month|day|hour|minute|second)s/i', '$1', $interval);

		if(preg_match('/^now$/i', $date)) {
			$date = "NOW()";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date)) {
			$date = "'$date'";
		}

		return "$date + INTERVAL $interval";
	}

	/**
	 * Function to return an SQL datetime expression that can be used with oci
	 * used for querying a datetime substraction
	 * @param string $date1, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @param string $date2 to be substracted of $date1, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @return string SQL datetime expression to query for the interval between $date1 and $date2 in seconds which is the result of the substraction
	 */
	function datetimeDifferenceClause($date1, $date2) {

		if(preg_match('/^now$/i', $date1)) {
			$date1 = "NOW()";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date1)) {
			$date1 = "'$date1'";
		}

		if(preg_match('/^now$/i', $date2)) {
			$date2 = "NOW()";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date2)) {
			$date2 = "'$date2'";
		}

		return "UNIX_TIMESTAMP($date1) - UNIX_TIMESTAMP($date2)";
	}
}

/**
 * A result-set from an Oracle database.
 * @package sapphire
 * @subpackage model
 */
class OracleQuery extends SS_Query {
	/**
	 * The OracleDatabase object that created this result set.
	 * @var OracleDatabase
	 */
	private $database;

	/**
	 * The internal oci handle that points to the result set.
	 * @var resource
	 */
	private $handle;

	/**
	 * Hook the result-set given into a Query class, suitable for use by sapphire.
	 * @param database The database object that created this query.
	 * @param handle the internal oci handle that is points to the resultset.
	 */
	public function __construct(OracleDatabase $database, $handle) {
		$this->database = $database;
		$this->handle = $handle;
	}

	public function __destroy() {
		oci_free_statement($this->handle);
	}

	public function seek($row) {
		trigger_error('not yet implemented');
	}

	public function numRecords() {
		return is_resource($this->handle) ? oci_num_rows($this->handle) : false;
	}

	public function nextRecord() {
		// Coalesce rather than replace common fields.
		if(is_resource($this->handle) && @$data = oci_fetch_row($this->handle)) {
			foreach($data as $columnIdx => $value) {
				$columnName = oci_field_name($this->handle, $columnIdx + 1);
				// $value || !$ouput[$columnName] means that the *last* occurring value is shown
				// !$ouput[$columnName] means that the *first* occurring value is shown
				if(isset($value) || !isset($output[$columnName])) {
					$output[$columnName] = $value;
				}
			}
			return $output;
		} else {
			return false;
		}
	}

}