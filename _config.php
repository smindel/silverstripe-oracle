<?php

OracleDatabase::$test_config = array(
	'username' => 'test',
	'password' => 'smindel',
	'server' => 'puss/XE',
);

// $oa = new OracleAdmin($databaseConfig);
// $oa = new OracleAdmin(OracleDatabase::$test_config);
// $oa->dropall();
// $oa->listnames();
// die('done');

class OracleAdmin {

	private $username;
	
	function __construct($param) {
		putenv("NLS_LANG=American_America.UTF8");
		$this->dbConn = oci_connect($param['username'], $param['password'], $param['server'], 'UTF8');
		$this->username = strtoupper($param['username']);
		if(!$this->dbConn) $this->databaseError("Couldn't connect to Oracle database");
		$this->query("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'");
	}
	
	function query($sql, $debug = false, $iknowwhatiamdoing = true) {
		if(!$iknowwhatiamdoing) $sql = preg_replace_callback('/"(\w{31,})"'.'(?=(?:(?:(?:[^\'\\\]++|\\.)*+\'){2})*+(?:[^\'\\\]++|\\.)*+$)/i', 'OracleDatabase::mapinvalifidentifiers', $sql);

		if($debug) Debug::dump($sql);
		
		$handle = oci_parse($this->dbConn, $sql);
		$success = oci_execute($handle);

		if(!$handle) {
			$error = oci_error();
			$this->databaseError("Couldn't run query: $sql | " . $error['message'], $errorLevel);
		}
		
		return $handle;
	}
	
	function get($query) {
		$res = $this->query($query);
		$out = array();
		while($row = oci_fetch_assoc($res)) $out[] = $row;
		return $out;
	}

	function one($query) {
		$res = $this->query($query);
		return oci_fetch_assoc($res);
	}

	function col($query, $col = null) {
		$res = $this->query($query);

		if(empty($col)) {
			$mode=OCI_NUM;
			$index=0;
		} else if(is_numeric($col)) {
			$mode=OCI_NUM;
			$index=$col;
		} else {
			$mode=OCI_ASSOC;
			$index=strtoupper($col);
		}

		$out = array();
		while($row = oci_fetch_array($res, $mode)) $out[] = $row[$index];
		return $out;
	}

	function val($query) {
		$res = $this->query($query);
		$row = oci_fetch_array($res, OCI_NUM);
		return $row[0];
	}

	function dropall($verbose = true) {
		$all = array(
			'trigger' => "select trigger_name from all_triggers where owner = '{$this->username}'",
			'sequence' => "select sequence_name from all_sequences where sequence_owner = '{$this->username}'",
			'table' => "select table_name from all_tables where owner = '{$this->username}'",
			'index' => "select index_name from all_indexes where owner = '{$this->username}'",
		);
		foreach($all as $type => $sql) foreach($this->col($sql) as $name) $this->query("drop $type \"$name\"", $verbose);
	}

	function listnames() {
		$all = array(
			'table' => "select * from all_tables where owner = '{$this->username}'",
			'sequence' => "select * from all_sequences where sequence_owner = '{$this->username}'",
			'trigger' => "select * from all_triggers where owner = '{$this->username}'",
			'index' => "select index_name from all_indexes where owner = '{$this->username}'",
		);
		foreach($all as $type => $sql) {
			Debug::dump(array($type, $this->col($sql, $type . '_name')));
		}
	}
	function listall() {
		$all = array(
			'table' => "select * from all_tables where owner = '{$this->username}'",
			'sequence' => "select * from all_sequences where sequence_owner = '{$this->username}'",
			'trigger' => "select * from all_triggers where owner = '{$this->username}'",
			'index' => "select index_name from all_indexes where owner = '{$this->username}'",
		);
		foreach($all as $type => $sql) {
			Debug::dump(array($type, $this->get($sql)));
		}
	}
}