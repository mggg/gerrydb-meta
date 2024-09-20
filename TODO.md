# Tests
Basic CRUD tests =
 * security (access scoped properly, no leaking of private namespaces)
 * simple write/read
 * simple error handling


# Features (ordered, short-term)


# Small improvements 
[ ] Find locations where path errors can occur and raise the GerryPathError rather than
    the current ValueError
[ ] Fix the logging on PUT calls


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
