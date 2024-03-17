module app;
version (norm_test){
	import norm;

	import std.stdio;
	import std.datetime;
	import std.conv : to;

	enum CONN_STR = "ddbc:sqlite:test.sqlite";

	class User : DBObject{
		public:
			enum Type{
				User,
				Admin,
				Management
			}

			@Val @Size(40) @Group("username") string username;
			@Val @Group("type") Type type;
			@Val uint year;

			override string toString() const pure {
				return "{username: " ~ username ~ ", type: " ~ type.to!string ~
					", year: " ~ year.to!string ~ "}";
			}

			this(string username, Type type, uint year){
				this.username = username;
				this.type = type;
				this.year = year;
			}
	}

	class Post : DBObject{
		public:
			@Val string title;
			@Val string content;
			@Val @Group("author") User author;

			override string toString() const pure {
				return "{title: " ~ title ~ ", content: " ~ content ~ ", author: " ~
					(author is null ? "null" : author.to!string) ~ "}";
			}

			this(string title, string content, User author = null){
				this.title = title;
				this.content = content;
				this.author = author;
			}
	}

	void test(){
		auto conn = connect(CONN_STR);

		assert (createTable!User(conn));
		assert (createTable!Post(conn));

		auto admin = new User("admin", User.Type.Admin, 2021),
				 manager = new User("manager", User.Type.Management, 2021),
				 user = new User("user", User.Type.User, 2020);

		assert(insert(conn, admin));
		assert(insert(conn, manager));
		assert(insert(conn, user));

		user.username = "uname";
		user.type = User.Type.Management;
		user.year = 2019;
		assert(update(conn, user));

		admin.username = "aadmin";
		assert(update!(User, "username")(conn, admin, "admin"));

		assert (dropTable!Post(conn));
		assert (dropTable!User(conn));
	}

	void main(){
		stderr.writeln("running norm tests");
		test;
	}
}
