module norm;

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
private template MembersWithVal(T){
	enum MembersWithVal = MemberNamesByUDA!(T, Val);
}

/// Member names that are of a specifc @Group name
private template MembersWithGroup(T, string name){
	enum MembersWithGroup = getMembersWithGroup;
	private string[] getMembersWithGroup(){
		string[] ret;
		static foreach (sym; getSymbolsByUDA!(T, Group)){{
			bool include = false;
			static foreach (group; getUDAs!(sym, Group))
				include = include || group.name == name;
			if (include)
				ret ~= sym.stringof;
		}}
		return ret;
	}
}

/// Group names
private template Groups(T){
	enum Groups = getGroups;
	private string[] getGroups(){
		string[] ret;
		static foreach (sym; getSymbolsByUDA!(T, Group)){
			static foreach (group; getUDAs!(sym, Group)){
				static if (!ret.canFind(group.name))
					ret ~= group.name;
			}
		}
		return ret;
	}
}

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
abstract class DBObject{
	ulong _normId;

public:
	/// Internal unique Id
	@property ulong id() pure const {
		return _normId;
	}
}

/// Create Table query
private template QueryCreate(T) if (is(T : DBObject)){
	enum QueryCreate = generateQuery;
	private string generateQuery(){
		string ret = "CREATE TABLE " ~ T.stringof ~ " (" ~
			"__normId BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, ";
		static foreach (sym; getSymbolsByUDA!(T, Val))
			ret ~= sym.stringof ~ " " ~ SQLType!sym ~ ", ";
		ret = ret.chomp(", ");
		return ret ~ ");";
	}
}

/// Drop Table query
private template QueryDropTable(T) if (is(T : DBObject)){
	enum QueryDropTable = "DROP TABLE " ~ T.stringof ~ ";";
}


/// Insert query
private template QueryInsert(T) if (is(T : DBObject)){
	enum QueryInsert = generateQuery;
	private string generateQuery(){
		string ret = "INSERT INTO " ~ T.stringof ~ " (";
		string post;
		static foreach (sym; getSymbolsByUDA!(T, Val)){
			ret ~= sym.stringof ~ ", ";
			post ~= "?, ";
		}
		ret = ret.chomp(", ") ~ ") VALUES (" ~ post.chomp(", ") ~ ");";
		return ret;
	}
}

/// Fetch all query
private template QueryFetchAll(T){
	enum QueryFetchAll = "SELECT __normId, " ~ MembersWithVal!T.join(", ") ~
		" FROM " ~ T.stringof ~ ";";
}

/// Fetch by id
private template QueryFetch(T){
	enum QueryFetch = QueryFetchAll!T.chomp(";") ~ " WHERE __normId=?;";
}

/// Fetch by a Group query
private template QueryFetch(T, string name){
	enum QueryFetch = QueryFetchAll!T.chomp(";") ~ " WHERE " ~
		MembersWithGroup!(T, name).join("=?, ") ~ "=?;";
}

/// Update all query
private template QueryUpdateAll(T){
	enum QueryUpdateAll = "UPDATE " ~ T.stringof ~ " SET " ~
		MembersWithVal!T.join("=?, ") ~ "=?;";
}

/// Update by id query
private template QueryUpdate(T){
	enum QueryUpdate = QueryUpdateAll!T.chomp(";") ~ " WHERE __normId=?;";
}

/// Update by a Group query
private template QueryUpdate(T, string name){
	enum QueryUpdate = QueryUpdateAll!T.chomp(";") ~ " WHERE " ~
		MembersWithGroup!(T, name).join("=?, ") ~ "=?;";
}

/// Drop all query
private template QueryDropAll(T){
	enum QueryDropAll = "DELETE FROM " ~ T.stringof ~ ";";
}

/// Drop by id query
private template QueryDrop(T){
	enum QueryDrop = QueryDropAll!T.chomp(";") ~ " WHERE __normId=?;";
}

/// Drop by a Group query
private template QueryDrop(T, string name){
	enum QueryDrop = QueryDropAll!T.chomp(";") ~ " WHERE " ~
		MembersWithGroup!(T, name).join("=?, ") ~ "=?;";
}

/// Count all query
private template QueryCountAll(T){
	enum QueryCountAll = "SELECT Count(*) FROM " ~ T.stringof ~ ";";
}

/// Count by keys query
private template QueryCount(T){
	enum QueryCount = QueryCountAll!T.chomp(";") ~ " WHERE __normId=?;";
}

/// Count by a Group query
private template QueryCount(T, string name){
	enum QueryCount = QueryCountAll!T.chomp(";") ~ " WHERE " ~
		MembersWithGroup!(T, name).join("=?, ") ~ "=?;";
}

struct Resuts(T) if (is(T : DBObject)){
private:
	SafeResultRange _range;
	Connection _conn;

	T _create(){
		if (_range.empty())
			return null;
		T obj = new T();
		static foreach (i, name; MembersWithVal!T){
			static if (is(typeof(__traits(getMember, T, name)) : DBObject)){
				// is a foreign key
				__traits(getMember, obj, name) =
					fetch!(typeof(__traits(getMember, T, name)))(
							_conn,
							cast(ulong)_range.front[i]);
			}else{
				__traits(getMember, obj, name) =
					cast(typeof(__traits(getMember, obj, name)))_range.front[i];
			}
		}
		return obj;
	}

	this(SafeResultRange range){
		_range = range;
	}

public:
	@disable this();
	bool empty(){
		return _range.empty;
	}

	void popFront(){
		return _range.popFront;
	}

	T front(){
		return _create;
	}
}

/// Creates a table
///
/// Returns: true if done, false if not
bool createTable(T)(Connection conn) if (is(T : DBObject)){
	try{
		exec(conn, QueryCreate!T);
		return true;
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return false;
	}
}

/// Inserts a object into table
///
/// Returns: true if done, false if not
bool insert(T)(Connection conn, ref T obj) if (is(T : DBObject)){
	try{
		MySQLVal[MembersWithVal!T.length] vals;
		static foreach (i, name; MembersWithVal!T)
			vals[i] = __traits(getMember, obj, name).toSQL;
		if (exec(conn, QueryInsert!T, vals))
			obj._normId = conn.lastInsertID;
		return true;
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return false;
	}
}

/// Updates a object
///
/// Returns: true if done, false if not
bool update(T)(Connection conn, T obj){
	try{
		MySQLVal[MembersWithVal!T.length + 1] vals;
		foreach (i, name; MembersWithVal!T)
			vals[i] = __traits(getMember, obj, name).toSQL;
		vals[$ - 1] = obj._normId.toSQL;
		exec(conn, QueryUpdate!T, vals);
		return true;
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return false;
	}
}

/// Fetches a object, by matching internal id
///
/// Returns: Results
T fetch(T)(Connection conn, ulong normId) if (is(T: DBObject)){
	try{
		return Resuts!T(query(conn, QueryFetch!T, [normId.toSQL])).front;
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return null;
	}
}

/// Fetches all objects.
Results!T fetch(T)(Connection conn) if (is(T : DBObject)){
	try{
		return Resuts!T(query(conn, QueryFetchAll!T));
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return null;
	}
}

/// Deletes object with matching internal id
///
/// Returns: true if done, false if not
bool drop(T)(Connection conn, ulong id) if (is(T : DBObject)){
	try{
		return exec(conn, QueryDrop!T, [normId.toSQL]);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return null;
	}
}

/// Deletes all objects
///
/// Returns: true if done, false if not
bool drop(T)(Connection conn) if (is(T : DBObject)){
	try{
		return exec(conn, QueryDropAll!T);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return null;
	}
}

/// Deletes table
///
/// Returns: true if done, false if not
bool dropTable(T)(Connection conn) if (is(T : DBObject)){
	try{
		return exec(conn, QueryDropTable!T);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return null;
	}
}

/// Returns: count of objects in table
ulong count(T)(Connection) if (is(T : DBObject)){
	try{
		SafeResultRange res = query(conn, QueryCountAll!T);
		if (res.empty) // ? wat
			return 0;
		SafeRow row = res.front;
		if (row.length == 0 || row.isNull(0)) // w a t
			return 0;
		return cast(ulong)row[0];
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return 0;
	}
}

/// Returns: true if object by an id exists
bool exists(T)(Connection conn, ulong normId) if (is(T : DBObject)){
	try{
		SafeResultRange res = query(conn, QueryCount!T, [normId.toSQL]);
		if (res.empty) // ? wat
			return false;
		SafeRow row = res.front;
		if (row.length == 0 || row.isNull(0)) // w a t
			return false;
		return cast(ulong)row[0] >= 1;
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return false;
	}
}

unittest{
	class A : DBObject{
		@Val uint id;
		@Val string name;
		@Val @Group("main") @Group("username") @Size(10) string username;
		@Val @Group("main") A other;
	}
	writefln!"create: %s"(QueryCreate!A);
	writefln!"insert: %s"(QueryInsert!A);

	writefln!"fetchAll: %s"(QueryFetchAll!A);
	writefln!"fetch: %s"(QueryFetch!A);
	writefln!"fetch \"main\": %s"(QueryFetch!(A, "main"));
	writefln!"fetch \"username\": %s"(QueryFetch!(A, "username"));

	writefln!"updateAll: %s"(QueryUpdateAll!A);
	writefln!"update: %s"(QueryUpdate!A);
	writefln!"update \"main\": %s"(QueryUpdate!(A, "main"));
	writefln!"update \"username\": %s"(QueryUpdate!(A, "username"));

	writefln!"dropAll: %s"(QueryDropAll!A);
	writefln!"drop: %s"(QueryDrop!A);
	writefln!"drop \"main\": %s"(QueryDrop!(A, "main"));
	writefln!"drop \"username\": %s"(QueryDrop!(A, "username"));

	writefln!"countAll: %s"(QueryCountAll!A);
	writefln!"count: %s"(QueryCount!A);
	writefln!"count \"main\": %s"(QueryCount!(A, "main"));
	writefln!"count \"username\": %s"(QueryCount!(A, "username"));
	writeln(typeof([5: 4, 4: 3]).stringof);
}
