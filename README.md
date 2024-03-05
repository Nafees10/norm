# norm

A simple ORM for use with D Language and SQLite (for now). Support for MYSQL,
PGSQL etc may arrive later.

norm provides simple functions to get started quickly with saving/retrieving
objects to/from SQL databases.

## Getting Started:

### Installation / Building

Run following to add norm to your project:

```base
dub add norm
```

in debug mode, norm will print all exceptions in DB facing functions, they will
not appear when built using `dub build -b=release`

### Connecting

Use the `connect(connectionString)` function to create a `NormConn` object.

```D
auto conn = connect("ddbc:sqlite:xyz.sqlite");
```

### Defining Objects

norm works on objects of classes inhering from `DBObject`. All data members
that norm needs to access should be public. Use the following attributes to
work with norm:

* `@Val` - Belongs in database, members without this are not touched by norm
* `@Size(length)` - tell length in cases of fixed length string.
* `@Group(nameString)` - group together multiple `@Val`s so fetching, updating,
	deleting, by matching them is possible.

`DBObject` provides a public `id` getter that returns the unique `ulong` id
that norm assigns to all objects.

A `@Val` field can by of type of another `DBObject` derived class as well.

### Creating Tables

```D
class User : DBObject{
public:
	enum Type{
		Admin,
		Moderator,
		User
	}

	@Val @Group("identity") string firstname;
	@Val @Group("identity") string lastname;
	@Val @Len(6) string registration;
	@Val Type type;
}

createTable!User(conn);
```

### Inserting

```D
auto user = new User();
user.x = y; // set values
insert(conn, user);
writefln!"user was assigned id: %d"(user.id);
```

### Updating

```D
user.type = User.Type.Admin;

// update this one user
update(conn, user);

// will turn all users into this one
updateAll(conn, user);

// match by group "identity" (firstname, lastname), turn them all into this one
update(User, "identity")(conn, user, "john", "doe");
```

### Fetching

```D
writeln("Enter user id");
ulong id = readln.chomp.to!string;
auto someone = fetch!User(conn, id);

// fetch all Users
auto allUsersRange = fetch!User(conn);
foreach (obj; allUsersRange)
	writeln(obj.firstname);

// fetch all that match identity
auto identityRange = fetch!(User, "identity")(conn, "john", "doe");
User[] johnDoes;
foreach (john; identityRange)
	johnDoes ~= john;
```

### Deleting

```D
// delete one user
drop(conn, user.id);

// delete by identity
drop!(User, "identity")(conn, "john", "doe");

// rm -rf them all
drop!User(conn);
```

### Counting

```D
// count all
auto userCount = count!User(conn);
writefln!"there are %d users"(userCount);

// count by id (will give 1 or 0)
userCount = count!User(conn, user.id);
writefln!"there are %d users with id %d"(userCount, user.id);
// alternatively, following work as well
if (exists!User(conn, user.id)) writeln(..);
if (exists(conn, user)) writeln(..);

// count by identity group
userCount = count!(User, "identity")(conn, "john", "doe");
writefln!"there are %d john does"(userCount);
```

### Deleting Table

```D
dropTable!User(conn);
```
