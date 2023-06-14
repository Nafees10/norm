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

/// true if a DBObject refers to at least 1 other
private template ContainsForeignKey(T) if (is(T : DBObject)){
	enum ContainsForeignKey = isComplexObject();
	private bool isComplexObject(){
		static foreach (sym; getSymbolsByUDA!(T, Val)){
			static if (is(typeof(sym) : DBObject))
				return true;
		}
		return false;
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
private:
	ulong _normId;

public:
	/// unique id
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

struct Results(T) if (is(T : DBObject)){
private:
	SafeResultRange _range;
	static if (ContainsForeignKey!T)
		Connection _conn;

	T _create(){
		if (_range.empty())
			return null;
		T obj = new T();
		static foreach (i, name; MembersWithVal!T){
			static if (ContainsForeignKey!T &&
					is(typeof(__traits(getMember, T, name)) : DBObject)){
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

	this(SafeResultRange range, Connection conn = null){
		_range = range;
		static if (ContainsForeignKey!T)
			_conn = conn;
	}

	this(){}

public:
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
		return exec(conn, QueryCreate!T) >= 1;
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
		if (exec(conn, QueryInsert!T, vals) == 0)
			return false;
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
		return exec(conn, QueryUpdate!T, vals) >= 1;
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return false;
	}
}

/// Updates all objects (i dont know why you would want to)
///
/// Returns: number of objects updated
ulong updateAll(T)(Connection conn, T obj){
	try{
		MySQLVal[MembersWithVal!T.length + 1] vals;
		foreach (i, name; MembersWithVal!T)
			vals[i] = __traits(getMember, obj, name).toSQL;
		return exec(conn, QueryUpdateAll!T, vals);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return 0;
	}
}

/// update by matching with a group name
///
/// Returns: number of objects updated
ulong update(string gName, T)(Connection conn, T match, T obj)
		if (is(T : DBObject)){
	try{
		MySQLVal[MembersWithVal!T.length + MembersWithGroup!(T, gName).length] vals;
		static foreach (i, name; MembersWithVal!T)
			vals[i] = __traits(getMember, obj, name).toSQL;
		static foreach (i, name; MembersWithGroup!(T, gName))
			vals[MembersWithVal!T.length + i] =
				__traits(getMember, match, name).toSQL;
		return exec(conn, QueryUpdate!(T, gName), vals);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return 0;
	}
}

/// ditto
ulong update(T, string gName)(Connection conn, T obj, ...)
		if (is(T : DBObject)){
	enum gCount = MembersWithGroup!(T, gName).length;
	static if (_arguments.length != gCount)
		static assert(false, "invalid number of parameters for group matching");

	try{
		MySQLVal[MembersWithVal!T + gCount] vals;
		foreach (i, name; MembersWithVal!T)
			vals[i] = __traits(getMember, obj, name).toSQL;
		foreach (i, name; MembersWithGroup!(T, gName)){
			static if (_arguments[i] != typeid(typeof(__traits(getMember, T, name))))
				static assert(false, "invalid type for " ~ name);
			vals[gCount + i] =
				va_arg!(typeof(__traits(getMember, T, name)))(_argptr).toSQL;
		}

		return exec(conn, QueryUpdate!(T, gName), vals);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return 0;
	}
}

/// Fetches a object, by matching internal id
///
/// Returns: Results
T fetch(T)(Connection conn, ulong id) if (is(T: DBObject)){
	try{
		return Results!T(query(conn, QueryFetch!T, [id.toSQL])).front;
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return null;
	}
}

/// Fetches by matching with a group
///
/// Returns: Results
Results!T fetch(string gName, T)(Connection conn, T match) if (
		is(T : DBObject)){
	try{
		MySQLVal[MembersWithGroup!(T, gName).length] vals;
		static foreach (i, name; MembersWithGroup!(T, gName))
			vals[i] = __traits(getMember, match, name).toSQL;
		return Results!T(query(conn, QueryFetch!(T, gName), vals), conn);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return Results!T;
	}
}

/// ditto
Results!T fetch(T, string gName)(Connection conn, ...) if (is(T : DBObject)){
	static if (_arguments.length != MembersWithGroup!(T, gName))
		static assert(false, "invalid number of parameters for group matching");

	try{
		MySQLVal[MembersWithGroup!(T, gName)] vals;
		foreach (i, name; MembersWithGroup!(T, gName)){
			static if (_arguments[i] != typeid(typeof(__traits(getMember, T, name))))
				static assert(false, "invalid type for " ~ name);
			vals[i] = va_arg!(typeof(__traits(getMember, T, name)))(_argptr).toSQL;
		}

		return Results!T(query(conn, QueryFetch!(T, gName), vals), conn);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return Results!T;
	}
}

/// Fetches all objects.
///
/// Returns: Results
Results!T fetch(T)(Connection conn) if (is(T : DBObject)){
	try{
		return Results!T(query(conn, QueryFetchAll!T), conn);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return Results!T;
	}
}

/// Deletes object with matching internal id
///
/// Returns: true if done, false if not
bool drop(T)(Connection conn, ulong id) if (is(T : DBObject)){
	try{
		return exec(conn, QueryDrop!T, [id.toSQL]) >= 1;
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return false;
	}
}

/// Deletes object(s) matcing a group
///
/// Returns: number of objects deleted
ulong drop(string gName, T)(Connection conn, T match) if (is(T : DBObject)){
	try{
		MySQLVal[MembersWithGroup!(T, gName).length] vals;
		static foreach (i, name; MembersWithGroup!(T, gName))
			vals[i] = __traits(getMember, match, name).toSQL;
		return exec(conn, QueryDrop!(T, gName), vals);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return 0;
	}
}

/// ditto
ulong drop(T, string gName)(Connection conn, ...) if (is(T : DBObject)){
	static if (_arguments.length != MembersWithGroup!(T, gName))
		static assert(false, "invalid number of parameters for group matching");

	try{
		MySQLVal[MembersWithGroup!(T, gName)] vals;
		foreach (i, name; MembersWithGroup!(T, gName)){
			static if (_arguments[i] != typeid(typeof(__traits(getMember, T, name))))
				static assert(false, "invalid type for " ~ name);
			vals[i] = va_arg!(typeof(__traits(getMember, T, name)))(_argptr).toSQL;
		}

		return exec(conn, QueryDrop!(T, gName), vals);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return 0;
	}
}

/// Deletes all objects
///
/// Returns: number of objects deleted
ulong drop(T)(Connection conn) if (is(T : DBObject)){
	try{
		return exec(conn, QueryDropAll!T);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return 0;
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
		return false;
	}
}

/// Returns: count of objects in table
ulong count(T)(Connection conn) if (is(T : DBObject)){
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

/// Returns: count of objects by a group
ulong count(string gName, T)(Connection conn, T match) if (is(T : DBObject)){
	try{
		MySQLVal[MembersWithGroup!(T, gName)] vals;
		static foreach (i, name; MembersWithGroup!(T, gName))
			vals[i] = __traits(getMember, match, name).toSQL;
		SafeResultRange res = query(conn, QueryCount!(T, gName));
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

/// ditto
ulong count(T, string gName)(Connection conn, ...) if (is(T : DBObject)){
	static if (_arguments.length != MembersWithGroup!(T, gName))
		static assert(false, "invalid number of parameters for group matching");

	try{
		MySQLVal[MembersWithGroup!(T, gName)] vals;
		foreach (i, name; MembersWithGroup!(T, gName)){
			static if (_arguments[i] != typeid(typeof(__traits(getMember, T, name))))
				static assert(false, "invalid type for " ~ name);
			vals[i] = va_arg!(typeof(__traits(getMember, T, name)))(_argptr).toSQL;
		}

		return exec(conn, QueryCount!(T, gName), vals);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return 0;
	}
}

/// Returns: true if object by an id exists
bool exists(T)(Connection conn, ulong id) if (is(T : DBObject)){
	try{
		SafeResultRange res = query(conn, QueryCount!T, [id.toSQL]);
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

/// Returns: true if object exists
bool exists(T)(Connection conn, T obj) if (is(T : DBObject)){
	return exists!T(conn, obj._normId);
}

unittest{
	class A : DBObject{
		@Val @Group("username") @Size(10) string username;
		@Val string name;
		@Val A other;
	}
	writefln!"create: %s"(QueryCreate!A);
	writefln!"insert: %s"(QueryInsert!A);

	writefln!"fetchAll: %s"(QueryFetchAll!A);
	writefln!"fetch: %s"(QueryFetch!A);
	writefln!"fetch by \"username\": %s"(QueryFetch!(A, "username"));

	writefln!"updateAll: %s"(QueryUpdateAll!A);
	writefln!"update: %s"(QueryUpdate!A);
	writefln!"update by \"username\": %s"(QueryUpdate!(A, "username"));

	writefln!"dropAll: %s"(QueryDropAll!A);
	writefln!"drop: %s"(QueryDrop!A);
	writefln!"drop by \"username\": %s"(QueryDrop!(A, "username"));

	writefln!"countAll: %s"(QueryCountAll!A);
	writefln!"count: %s"(QueryCount!A);
	writefln!"count by \"username\": %s"(QueryCount!(A, "username"));
}
