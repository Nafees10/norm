module norm;

import utils.misc;

import std.string,
			 std.array,
			 std.algorithm,
			 std.traits,
			 std.meta,
			 std.conv,
			 std.datetime,
			 std.stdio;

import core.vararg;

/// UDA. label as Database column
enum Val;
/// UDA. mark with size. use with string to make `VARCHAR(x)` etc
struct Size{ ulong len; }
/// Selection group
struct Group{ string name; }

/// Member names that have @Val
private template MembersWithVal(T){
	alias MembersWithVal = AliasSeq!();
	static foreach (sym; getSymbolsByUDA!(T, Val))
		MembersWithVal = AliasSeq!(MembersWithVal, sym.stringof);
}

/// Member names that are of a specifc @Group name
private template MembersWithGroup(T, string name){
	alias MembersWithGroup = AliasSeq!();
	static foreach (sym; getSymbolsByUDA!(T, Group)){
		static if (anySatisfy!(GroupMatches, getUDAs!(sym, Group)))
			MembersWithGroup = AliasSeq!(MembersWithGroup, sym.stringof);
	}
	private enum GroupMatches(alias G) = G.name == name;
}

/// Group names
private template Groups(T){
	alias Groups = AliasSeq!();
	static foreach (sym; getSymbolsByUDA!(T, Group)){
		static foreach (group; getUDAs!(sym, Group))
			Groups = NoDuplicates!(Groups, group.name);
	}
}

/// Whether `typeof(T) : DBOject`
private template IsDBObject(alias T) {
	enum IsDBObj = is(typeof(T) : DBObject);
}

/// true if a DBObject refers to at least 1 other
private template ContainsForeignKey(T) if (is(T : DBObject)){
	enum ContainsForeignKey = anySatisfy!(IsDBObj, getSymbolsByUDA!(T, Val));
}

/// Member names that are foreign keys
private template MembersWithFKey(T) if (is(T : DBObject)){
	alias MembersWithFKey = AliasSeq!();
	static foreach (sym; getSymbolsByUDA!(T, Val)){
		static if (IsDBObject!(sym))
			MembersWithFKey = AliasSeq!(MembersWithFKey, sym.stringof);
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
	static if (hasUDA!(sym, Size) &&
			(is (typeof(sym) == string) ||
			 is (typeof(sym) == char[])))
		enum SQLType = "VARCHAR(" ~ getUDAs!(sym, Size)[0].len.to!string ~ ")";
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

	/// unique id
	@property ulong id(ulong val) pure {
		return _normId = val;
	}

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
		static if (ContainsForeignKey!T){
			static foreach (name; MembersWithFKey!T){
				ret ~= "FOREIGN KEY (" ~ name ~ ") REFERENCES " ~
					typeof(__traits(getMember, T, name)).stringof ~ "(__normId), ";
			}
		}
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

/// Connects using a connection string
///
/// A connection string looks like:
/// `"host=localhost;port=3306;user=dummy;pwd=dummy;db=dummy"`
Connection connect(string str){
	return new Connection(str);
}

public struct Results(T) if (is(T : DBObject)){
private:
	SafeResultRange _range;
	static if (ContainsForeignKey!T)
		Connection _conn;

	T _create(){
		if (_range.empty())
			return null;
		T obj = new T();
		obj.id = cast(ulong)_range.front[0];
		static foreach (i, name; MembersWithVal!T){ // the i + 1 is coz 0 is normId
			static if (ContainsForeignKey!T &&
					is(typeof(__traits(getMember, T, name)) : DBObject)){
				// is a foreign key
				if (!_range.front.isNull(i + 1))
					__traits(getMember, obj, name) =
						fetch!(typeof(__traits(getMember, T, name)))(
								_conn, cast(ulong)_range.front[i + 1]);
			}else{
				__traits(getMember, obj, name) =
					cast(typeof(__traits(getMember, T, name)))_range.front[i + 1];
			}
		}
		return obj;
	}

	this(SafeResultRange range, Connection conn = null){
		_range = range;
		static if (ContainsForeignKey!T)
			_conn = conn;
	}

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
		if (exec(conn, QueryInsert!T, vals) == 0)
			return false;
		obj.id = conn.lastInsertID;
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
		static foreach (i, name; MembersWithVal!T)
			vals[i] = __traits(getMember, obj, name).toSQL;
		vals[$ - 1] = obj.id.toSQL;
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
/// Parameters are fields of that group, in the order they are defined.
/// Note that although matching is done by group fields, any object that matches
/// is updated whole
///
/// Returns: number of objects updated
ulong update(T, string gName, Types...)(Connection conn, T obj, Types args) if (
		is (T : DBObject)){
	static assert(Types.length == MembersWithGroup!(T, gName).length,
			"unexpected number of arguments against group field members");
	static foreach (i, name; MembersWithGroup!(T, gName))
		static assert(is (typeof(__traits(getMember, T, name)) : Types[i]),
				"Types do not match group field member types");

	try{
		MySQLVal[MembersWithVal!T.length + MembersWithGroup!(T, gName).length] vals;
		static foreach (i, name; MembersWithVal!T)
			vals[i] = __traits(getMember, obj, name).toSQL;
		static foreach (i; 0 .. args.length)
			vals[MembersWithVal!T.length + i] = args[i].toSQL;
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
Results!T fetch(T, string gName, Types...)(Connection conn, Types args) if (
		is(T : DBObject)){
	static assert(Types.length == MembersWithGroup!(T, gName).length,
			"unexpected number of arguments against group field members");
	static foreach (i, name; MembersWithGroup!(T, gName))
		static assert(is (typeof(__traits(getMember, T, name)) : Types[i]),
				"types do not match group field member types");

	try{
		MySQLVal[MembersWithGroup!(T, gName)] vals;
		static foreach (i; 0 .. args.length)
			vals[i] = args[i].toSQL;

		return Results!T(query(conn, QueryFetch!(T, gName), vals), conn);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return Results!T();
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
		return Results!T();
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
ulong drop(T, string gName, Types...)(Connection conn, Types args) if (
			is(T : DBObject)){
	static assert(Types.length == MembersWithGroup!(T, gName).length,
			"unexpected number of arguments against group field members");
	static foreach (i, name; MembersWithGroup!(T, gName))
		static assert(is (typeof(__traits(getMember, T, name)) : Types[i]),
				"types do not match group field member types");

	try{
		MySQLVal[MembersWithGroup!(T, gName)] vals;
		static foreach (i; 0 .. args.length)
			vals[i] = args[i].toSQL;

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
		exec(conn, QueryDropTable!T);
		return true;
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
ulong count(T, string gName, Types...)(Connection conn, Types args) if (
			is(T : DBObject)){
	static assert(Types.length == MembersWithGroup!(T, gName).length,
			"unexpected number of arguments against group field members");
	static foreach (i, name; MembersWithGroup!(T, gName))
		static assert(is (typeof(__traits(getMember, T, name)) : Types[i]),
				"Types do not match group field member types");

	try{
		MySQLVal[MembersWithGroup!(T, gName)] vals;
		foreach (i; 0 .. args.length)
			vals[i] = args[i].toSQL;

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
	return exists!T(conn, obj.id);
}

unittest{
	class User : DBObject{
		public:
			enum Type{
				User,
				Admin,
				Management
			}
			@Val @Size(40) @Group("username") string username;
			@Val @Group("type") Type type;
	}

	class Post : DBObject{
		public:
			@Val DateTime time;
			@Val string title;
			@Val string content;
			@Val @Group("author") User author;
	}

	writefln!"create: %s"(QueryCreate!User);
	writefln!"insert: %s"(QueryInsert!User);

	writefln!"fetchAll: %s"(QueryFetchAll!User);
	writefln!"fetch: %s"(QueryFetch!User);
	writefln!"fetch by \"username\": %s"(QueryFetch!(User, "username"));

	writefln!"updateAll: %s"(QueryUpdateAll!User);
	writefln!"update: %s"(QueryUpdate!User);
	writefln!"update by \"username\": %s"(QueryUpdate!(User, "username"));

	writefln!"dropAll: %s"(QueryDropAll!User);
	writefln!"drop: %s"(QueryDrop!User);
	writefln!"drop by \"username\": %s"(QueryDrop!(User, "username"));

	writefln!"countAll: %s"(QueryCountAll!User);
	writefln!"count: %s"(QueryCount!User);
	writefln!"count by \"username\": %s"(QueryCount!(User, "username"));
}
