module tablemaker.dbase;

import utils.misc;

import mysql.safe;

import std.string,
			 std.array,
			 std.algorithm,
			 std.traits,
			 std.conv,
			 std.datetime,
			 std.stdio;

import core.vararg;

/// UDA. label as Database column
enum Val;
/// UDA. label as key. can be used with multiple
enum Key;
/// UDA. auto increment
enum Auto;
/// UDA. enable integer mangling
enum Mangle;
/// UDA. mark with size. use with string to make `VARCHAR(x)` etc
struct Size{ ulong len; }
/// Fetch group
struct Group{ string name; }

/// names of members with a specific UDA
private template MemberNamesByUDA(T, UDA){
	enum MemberNamesByUDA = getMemberNamesByUDA;
	private string[] getMemberNamesByUDA(){
		string[] ret;
		static foreach (sym; getSymbolsByUDA!(T, UDA))
			ret ~= sym.stringof;
		return ret;
	}
}

/// Member names that have @Val
private enum Vals(T) = MemberNamesByUDA!(T, Val);
/// Member names that have @Key
private enum Keys(T) = MemberNamesByUDA!(T, Key);
/// Member names that have @Auto
private enum Autos(T) = MemberNamesByUDA!(T, Auto);
/// Member names that have @Mangle
private enum Mangles(T) = MemberNamesByUDA!(T, Mangle);
///

/// SQL types
private template SQLTypeMap(T){
	static if (is (T == bool))
		enum SQLTypeMap = "BIT";
	else static if (is (T == ubyte) ||
			is (T == ushort) ||
			is (T == uint))
		enum SQLTypeMap = "INT UNSIGNED";
	else static if (is (T == byte) ||
			is (T == short) ||
			is (T == int))
		enum SQLTypeMap = "INT";
	else static if (is (T == ulong))
		enum SQLTypeMap = "BIGINT UNSIGNED";
	else static if (is (T == long))
		enum SQLTypeMap = "BIGINT";
	else static if (is (T == float))
		enum SQLTypeMap = "FLOAT";
	else static if (is (T == double))
		enum SQLTypeMap = "DOUBLE";
	else static if (is (T == DateTime))
		enum SQLTypeMap = "DATETIME";
	else static if (is (T == Date))
		enum SQLTypeMap = "DATE";
	else static if(is (T == TimeOfDay))
		enum SQLTypeMap = "TIME";
	else static if (is (T == string) || is (T == char[]))
		enum SQLTypeMap = "TEXT";
	else static if (is (T == enum))
		enum SQLTypeMap = SQLTypeMap!(OriginalType!(Unqual!T));
	else static if (is (T : DBObject))
		enum SQLTypeMap = "BIGINT UNSIGNED";
	else
		static assert (false, "unsupported type in SQLTypeMap: " ~ name);
}

/// get SQL type name
private template SQLType(alias sym){
	static if (hasUDA!(sym, Size) && (
				is (typeof(sym) == string) ||
				is (typeof(sym) == char[])))
		enum SQLType = "VARCHAR(" ~ getUDAs!(sym, Size)[0].
			len.to!string ~ ")";
	else
		enum SQLType = SQLTypeMap!(typeof(sym));
}

/// converts to MySQLVal
MySQLVal toSQL(From)(From val) pure {
	static if (is (From == bool))
		return MySQLVal(cast(int)(val * 1));
	else static if (is (From == ubyte) ||
			is (From == ushort) ||
			is (From == uint))
		return MySQLVal(cast(uint)val);
	else static if (is (From == byte) ||
			is (From == short) ||
			is (From == int))
		return MySQLVal(cast(int)val);
	else static if (is (From == ulong))
		return MySQLVal(cast(ulong)val);
	else static if (is (From == long))
		return MySQLVal(cast(long)val);
	else static if (is (From == float))
		return MySQLVal(cast(float)val);
	else static if (is (From == double))
		return MySQLVal(cast(double)val);
	else static if (is (From == enum))
		return MySQLVal(cast(OriginalType!(Unqual!From))val);
	else
		return MySQLVal(val);
}

/// Parent class for all DBObjects
abstract class DBObject{}

/// Create Table query
private template QueryCreateTable(T) if (is(T : DBObject)){
	enum QueryCreateTable = generateQuery;
	private string generateQuery(){
		string ret = "CREATE TABLE " ~ T.stringof ~ " (";
		static foreach (sym; getSymbolsByUDA!(T, Val)){
			ret ~= sym.stringof ~ " " ~ SQLType!sym;
			static if (hasUDA!(sym, Auto))
				ret ~= " AUTO_INCREMENT";
			ret ~= ", ";
		}
		static if (getSymbolsByUDA!(T, Key).length){
			ret ~= "PRIMARY KEY (";
			static foreach (sym; getSymbolsByUDA!(T, Key))
				ret ~= sym.stringof ~ ", ";
			ret = ret.chomp(", ") ~ ")";
		}else{
			ret = ret.chomp(", ");
		}
		return ret ~ ");";
	}
}

/// Insert query
private template QueryInsert(T) if (is(T : DBObject)){
	enum QueryInsert = generateQuery;
	private string generateQuery(){
		string ret = "INSERT INTO " ~ T.stringof ~ " (";
		string post;
		// insert those that are not Auto
		static foreach (sym; getSymbolsByUDA!(T, Val)){
			static if (!hasUDA!(sym, Auto)){
				ret ~= sym.stringof ~ ", ";
				post ~= "?, ";
			}
		}
		ret = ret.chomp(", ") ~ ") VALUES (" ~ post.chomp(", ") ~ ");";
		return ret;
	}
}

/// Fetch all
private template QueryFetchAll(T){
	enum QueryFetchAll = generateQuery;
	private string generateQuery(){
		string ret = "SELECT ";
		static foreach (sym; getSymbolsByUDA!(T, Val))
			ret ~= sym.stringof ~ ", ";
		ret = ret.chomp(", ") ~ " FROM " ~ T.stringof ~ ";";
		return ret;
	}
}

/// Fetch By keys
private template QueryFetch(T){
	enum QueryFetch = generateQuery;
	private string generateQuery(){
		string ret = QueryFetchAll!T.chomp(";") ~ " WHERE ";
		static foreach (sym; getSymbolsByUDA!(T, Key))
			ret ~= sym.stringof ~ "=?, ";
		ret = ret.chomp(", ") ~ ";";
		return ret;
	}
}

/// Fetch By a Group
private template QueryFetch(T, string name){
	enum QueryFetch = generateQuery;
	private string generateQuery(){
		string ret = QueryFetchAll!T.chomp(";") ~ " WHERE ";
		static foreach (sym; getSymbolsByUDA!(T, Group)){{
			bool include = false;
			static foreach (group; getUDAs!(sym, Group))
				include = include || group.name == name;
			if (include)
				ret ~= sym.stringof ~ "=?, ";
		}}
		ret = ret.chomp(", ") ~ ";";
		return ret;
	}
}

unittest{
	class A : DBObject{
		@Val @Key @Auto @Mangle uint id;
		@Val string name;
		@Val @Group("main") @Group("username") @Size(10) string username;
		@Val @Group("main") A other;
	}
	writeln(QueryFetch!(A, "username"));
}
