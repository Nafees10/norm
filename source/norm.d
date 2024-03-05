module norm;

import std.string,
			 std.array,
			 std.algorithm,
			 std.traits,
			 std.meta,
			 std.conv,
			 std.datetime,
			 std.stdio;

import core.vararg;

import ddbc;

/// UDA. label as Database column
enum Val;
/// UDA. mark with size. use with string to make `VARCHAR(x)` etc
struct Size{ ulong len; }
/// Selection group
struct Group{ string name; }

/// joins AliasSeq of strings with a joinstring: `(..., joinstring)`
private template Join(S...){
	enum Join = joinedStr();
	string joinedStr(){
		string ret;
		static if (S.length > 3){
			static foreach (s; S[0 .. $ - 2])
				ret ~= s ~ S[$ - 1];
		}
		ret ~= S[$ - 2];
		return ret;
	}
}

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
	enum IsDBObject = is(typeof(T) : DBObject);
}

/// true if a DBObject refers to at least 1 other
private template ContainsForeignKey(T) if (is(T : DBObject)){
	enum ContainsForeignKey = anySatisfy!(IsDBObject, getSymbolsByUDA!(T, Val));
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

private void set(T)(PreparedStatement stmnt, int index, T val){
	static if (is (T == bool))
		stmnt.setBoolean(index, val);
	else static if (is (T == ubyte) ||
			is (T == ushort) ||
			is (T == uint))
		stmnt.setUint(index, val);
	else static if (is (T == byte) ||
			is (T == short) ||
			is (T == int))
		stmnt.setInt(index, val);
	else static if (is (T == ulong))
		stmnt.setUlong(index, val);
	else static if (is (T == long))
		stmnt.setLong(index, val);
	else static if (is (T == float))
		stmnt.setFloat(index, val);
	else static if (is (T == double))
		stmnt.setDouble(index, val);
	else static if (is (T == DateTime))
		stmnt.setDateTime(index, val);
	else static if (is (T == Date))
		stmnt.setDate(index, val);
	else static if(is (T == TimeOfDay))
		stmnt.setTime(index, val);
	else static if (is (T == string) || is (T == char[]))
		stmnt.setString(index, val);
	else static if (is (T == enum))
		stmnt.set!(OriginalType!(Unqual!T))(index, val);
	else static if (is (T : DBObject))
		stmnt.setUlong(index, val.__normId);
	else
		static assert (false, "unsupported type in norm.set: " ~ name);
}

private T get(T)(ResultSet res, int index){
	static if (is (T == bool))
		return res.getBoolean(index);
	else static if (is (T == ubyte) ||
			is (T == ushort) ||
			is (T == uint))
		return res.getUint(index);
	else static if (is (T == byte) ||
			is (T == short) ||
			is (T == int))
		return res.getInt(index);
	else static if (is (T == ulong))
		return res.getUlong(index);
	else static if (is (T == long))
		return res.getLong(index);
	else static if (is (T == float))
		return res.getFloat(index);
	else static if (is (T == double))
		return res.getDouble(index);
	else static if (is (T == DateTime))
		return res.getDateTime(index);
	else static if (is (T == Date))
		return res.getDate(index);
	else static if(is (T == TimeOfDay))
		return res.getTime(index);
	else static if (is (T == string) || is (T == char[]))
		return res.getString(index);
	else static if (is (T == enum))
		return res.get!(SQLTypeMap!(OriginalType!(Unqual!T)))(index);
	else static if (is (T : DBObject))
		static assert (false, "cannot use norm.get to get DBOject");
	else
		static assert (false, "unsupported type in norm.get: " ~ name);
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
			"__normId INTEGER PRIMARY KEY, ";
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
	enum QueryFetchAll = "SELECT __normId, " ~ Join!(MembersWithVal!T, ", ") ~
		" FROM " ~ T.stringof ~ ";";
}

/// Fetch by id
private template QueryFetch(T){
	enum QueryFetch = QueryFetchAll!T.chomp(";") ~ " WHERE __normId=?;";
}

/// Fetch by a Group query
private template QueryFetch(T, string name){
	enum QueryFetch = QueryFetchAll!T.chomp(";") ~ " WHERE " ~
		Join!(MembersWithGroup!(T, name), "=?, ") ~ "=?;";
}

/// Update all query
private template QueryUpdateAll(T){
	enum QueryUpdateAll = "UPDATE " ~ T.stringof ~ " SET " ~
		Join!(MembersWithVal!T, "=?, ") ~ "=?;";
}

/// Update by id query
private template QueryUpdate(T){
	enum QueryUpdate = QueryUpdateAll!T.chomp(";") ~ " WHERE __normId=?;";
}

/// Update by a Group query
private template QueryUpdate(T, string name){
	enum QueryUpdate = QueryUpdateAll!T.chomp(";") ~ " WHERE " ~
		Join!(MembersWithGroup!(T, name), "=?, ") ~ "=?;";
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
		Join!(MembersWithGroup!(T, name), "=?, ") ~ "=?;";
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
		Join!(MembersWithGroup!(T, name), "=?, ") ~ "=?;";
}

/// Norm Connection (a wrapper around DDBC Connection)
public class NormConn : Connection{
private:
	PreparedStatement[string] _stmnt; /// prepared statements
public:
	this(Connection conn){
		this._conn = conn;
		stmnt = conn.createStatement();
	}
	Connection _conn; /// ddbc connection
	Statement stmnt;
	DialectType getDialectType(){
		return _conn.getDialectType();
	}
	void close(){
		foreach (query, st; _stmnt)
			st.close();
		_conn.close();
	}
	void commit(){
		_conn.commit();
	}
	string getCatalog(){
		return _conn.getCatalog();
	}
	void setCatalog(string catalog){
		_conn.setCatalog(catalog);
	}
	bool isClosed(){
		return _conn.isClosed();
	}
	void rollback(){
		_conn.rollback();
	}
	bool getAutoCommit(){
		return _conn.getAutoCommit();
	}
	void setAutoCommit(bool autoCommit){
		_conn.setAutoCommit(autoCommit);
	}
	Statement createStatement(){
		return _conn.createStatement();
	}
	PreparedStatement prepareStatement(string query){
		if (auto val = query in _stmnt){
			return *val;
		}
		PreparedStatement st = _conn.prepareStatement(query);
		_stmnt[query] = st;
		return st;
	}
	TransactionIsolation getTransactionIsolation(){
		return _conn.getTransactionIsolation();
	}
	void setTransactionIsolation(TransactionIsolation level){
		_conn.setTransactionIsolation(level);
	}
}

/// Connects using a connection string
///
/// See DDBC connecting strings
NormConn connect(string url){
	return new NormConn(createConnection(url));
}

public struct Results(T) if (is(T : DBObject)){
private:
	ResultSet _res;
	bool over = false;
	static if (ContainsForeignKey!T)
		NormConn _conn;

	T _create(){
		if (over)
			return null;
		try{
			T obj = new T();
			obj.id = _res.get!ulong(1);
			static foreach (i, name; MembersWithVal!T){ // the i + 1 is coz 0 is normId
				static if (is(typeof(__traits(getMember, T, name)) : DBObject)){
					// is a foreign key
					if (!_res.isNull(i + 2))
						__traits(getMember, obj, name) =
							fetch!(typeof(__traits(getMember, T, name)))(
									_conn, _res.getUlong(i + 2));
				}else{
					// is a value
					__traits(getMember, obj, name) =
						_res.get!(typeof(__traits(getMember, obj, name)))(i + 2);
				}
			}
			return obj;
		} catch (Exception e){
			debug stderr.writeln(e.msg);
			return null;
		}
	}

	this(ResultSet res, NormConn conn = null){
		_res = res;
		static if (ContainsForeignKey!T)
			_conn = conn;
		over = !_res.next();
	}

public:
	bool empty(){
		return over;
	}
	void popFront(){
		over = !_res.next();
	}
	T front(){
		return _create;
	}
}

/// Creates a table
///
/// Returns: true if done, false if not
bool createTable(T)(NormConn conn) if (is(T : DBObject)){
	try{
		conn.stmnt.executeUpdate(QueryCreate!T);
		return true;
		//return conn.stmnt.executeUpdate(QueryCreate!T) == 1;
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return false;
	}
}

/// Inserts a object into table
///
/// Returns: true if done, false if not
bool insert(T)(NormConn conn, ref T obj) if (is(T : DBObject)){
	try{
		PreparedStatement stmnt = conn.prepareStatement(QueryInsert!T);
		static foreach (i, name; MembersWithVal!T)
			stmnt.set(i + 1, __traits(getMember, obj, name));
		import std.variant : Variant;
		Variant id;
		if (stmnt.executeUpdate(id) == 0)
			return false;
		obj.id = id.get!ulong;
		return true;
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return false;
	}
}

/// Updates a object
///
/// Returns: true if done, false if not
bool update(T)(NormConn conn, T obj) if (is (T : DBObject)){
	try{
		PreparedStatement stmnt = conn.prepareStatement(QueryUpdate!T);
		static foreach (i, name; MembersWithVal!T)
			stmnt.set(i + 1, __traits(getMember, obj, name));
		stmnt.setUlong(MembersWithVal!T.length + 1, obj.id);
		return stmnt.executeUpdate() >= 1;
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return false;
	}
}

/// Updates all objects (i dont know why you would want to)
///
/// Returns: number of objects updated
ulong updateAll(T)(NormConn conn, T obj) if (is (T : DBObject)){
	try{
		PreparedStatement stmnt = conn.prepareStatement(QueryUpdate!T);
		static foreach (i, name; MembersWithVal!T)
			stmnt.set(i + 1, __traits(getMember, obj, name));
		return stmnt.executeUpdate() >= 1;
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
ulong update(T, string gName, Types...)(NormConn conn, T obj, Types args)
		if (is (T : DBObject)){
	static assert(Types.length == MembersWithGroup!(T, gName).length,
			"unexpected number of arguments against group field members");
	static foreach (i, name; MembersWithGroup!(T, gName))
		static assert(is (typeof(__traits(getMember, T, name)) : Types[i]),
				"Types do not match group field member types");

	try{
		PreparedStatement stmnt = conn.prepareStatement(QueryUpdate!(T, gName));
		static foreach (i, name; MembersWithVal!T)
			stmnt.set(i + 1, __traits(getMember, obj, name));
		static foreach (i; 0 .. args.length)
			stmnt.set(MembersWithVal!T.length + i + 1, args[i]);
		return stmnt.executeUpdate();
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return 0;
	}
}

/// Fetches a object, by matching internal id
///
/// Returns: Results
T fetch(T)(NormConn conn, ulong id) if (is(T: DBObject)){
	try{
		PreparedStatement stmnt = conn.prepareStatement(QueryFetch!T);
		stmnt.setUlong(1, id);
		return Results!T(stmnt.executeQuery(), conn).front;
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return null;
	}
}

/// Fetches by matching with a group
///
/// Returns: Results
Results!T fetch(T, string gName, Types...)(NormConn conn, Types args)
		if (is(T : DBObject)){
	static assert(Types.length == MembersWithGroup!(T, gName).length,
			"unexpected number of arguments against group field members");
	static foreach (i, name; MembersWithGroup!(T, gName))
		static assert(is (typeof(__traits(getMember, T, name)) : Types[i]),
				"types do not match group field member types");

	try{
		PreparedStatement stmnt = conn.prepareStatement(QueryFetch!(T, gName));
		static foreach (i; 0 .. args.length)
			stmnt.set(i + 1, args[i]);
		return Results!T(stmnt.executeQuery(), conn);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return Results!T();
	}
}

/// Fetches all objects.
///
/// Returns: Results
Results!T fetch(T)(NormConn conn) if (is(T : DBObject)){
	try{
		return Results!T(conn.stmnt.executeQuery(QueryFetchAll!T), conn);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return Results!T();
	}
}

/// Deletes object with matching internal id
///
/// Returns: true if done, false if not
bool drop(T)(NormConn conn, ulong id) if (is(T : DBObject)){
	try{
		PreparedStatement stmnt = conn.prepareStatement(QueryDrop!T);
		stmnt.setUlong(1, id);
		return stmnt.executeUpdate() >= 1;
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return false;
	}
}

/// Deletes object(s) matcing a group
///
/// Returns: number of objects deleted
ulong drop(T, string gName, Types...)(NormConn conn, Types args)
		if (is(T : DBObject)){
	static assert(Types.length == MembersWithGroup!(T, gName).length,
			"unexpected number of arguments against group field members");
	static foreach (i, name; MembersWithGroup!(T, gName))
		static assert(is (typeof(__traits(getMember, T, name)) : Types[i]),
				"types do not match group field member types");

	try{
		PreparedStatement stmnt = conn.prepareStatement(QueryDrop!(T, gName));
		static foreach (i; 0 .. args.length)
			stmnt.set(i + 1, args[i]);
		return stmnt.executeUpdate();
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return 0;
	}
}

/// Deletes all objects
///
/// Returns: number of objects deleted
ulong drop(T)(NormConn conn) if (is(T : DBObject)){
	try{
		return conn.stmnt.executeQuery(QueryDropAll!T);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return 0;
	}
}

/// Deletes table
///
/// Returns: true if done, false if not
bool dropTable(T)(NormConn conn) if (is(T : DBObject)){
	try{
		return conn.stmnt.executeUpdate(QueryDropTable!T) >= 1;
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return false;
	}
}

/// Returns: count of objects in table
ulong count(T)(NormConn conn) if (is(T : DBObject)){
	try{
		ResultSet res = conn.stmnt.executeQuery(QueryCountAll!T);
		if (!res.next())
			return 0; // ? wat
		return res.getLong(1);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return 0;
	}
}

/// Returns: count of objects by a group
ulong count(T, string gName, Types...)(NormConn conn, Types args)
		if (is(T : DBObject)){
	static assert(Types.length == MembersWithGroup!(T, gName).length,
			"unexpected number of arguments against group field members");
	static foreach (i, name; MembersWithGroup!(T, gName))
		static assert(is (typeof(__traits(getMember, T, name)) : Types[i]),
				"Types do not match group field member types");

	try{
		PreparedStatement stmnt = conn.prepareStatement(QueryCount!(T, gName));
		foreach (i; 0 .. args.length)
			stmnt.set(i + 1, args[i]);

		ResultSet res = stmnt.executeQuery();
		if (!res.next())
			return 0; // ? wat
		return res.getLong(1);
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return 0;
	}
}

/// Returns: true if object by an id exists
bool exists(T)(NormConn conn, ulong id) if (is(T : DBObject)){
	try{
		PreparedStatement stmnt = conn.prepareStatement(QueryCount!T);
		stmnt.setUlong(1, id);

		ResultSet res = stmnt.executeQuery();
		if (!res.next())
			return 0; // ? wat
		return res.getLong(1) == 1;
	}catch (Exception e){
		debug stderr.writeln(e.msg);
		return false;
	}
}

/// Returns: true if object exists
bool exists(T)(NormConn conn, T obj) if (is(T : DBObject)){
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
