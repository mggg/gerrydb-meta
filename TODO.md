# Tests
Basic CRUD tests =
 * security (access scoped properly, no leaking of private namespaces)
 * simple write/read
 * simple error handling

[ ] Basic CRUD tests
    [ ] Columns
    [ ] Column sets
    [ ] Geographic layers
    [ ] ...add more here...

# Features (ordered, short-term)
[ ] change CLI to grant appropraite (full) privileges to user who initializes database



Expose:
[x] GeoImport / GeoLayer
[ ] Geography (with versioning)
[ ] GeoSet (with versioning)
[ ] ColumnValue/column data imports (with versioning)
[ ] ViewTemplate
[ ] View

...then pivot to clients/admin. Later:
[ ] User/group/role management
[ ] ETL infrastructure (separate repo)
[ ] Minimal admin panel, probably by hacking up [aminalaee/sqladmin](https://github.com/aminalaee/sqladmin)
