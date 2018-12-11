
# tap-toggl

Tap for [Toggl](https://www.toggl.com/).

## Requirements

- pip3
- python 3.5+
- mkvirtualenv

## Installation

```
$ mkvirtualenv -p python3 tap-toggl
$ pip3 install tap-toggl
```

## Usage

### Create config file

This config is to authenticate into toggl. You can request an API token in your settings on the Toggl website.

The `detailed_report_trailing_days` determines the window of how many trailing days to pull the `time_entries` resource.

```
{
  "api_token": "*****",
  "detailed_report_trailing_days": 1
}
```

### Discovery mode

This command returns a JSON that describes the schema of each table.

```
$ tap-toggl --config config.json --discover
```

To save this to `catalog.json`:

```
$ tap-toggl --config config.json --discover > catalog.json
```

### Field selection

You can tell the tap to extract specific fields by editing `catalog.json` to make selections. Note the top-level `selected` attribute, as well as the `selected` attribute nested under each property.

```
{
  "selected": "true",
  "properties": {
    "likes_getting_petted": {
      "selected": "true",
      "inclusion": "available",
      "type": [
        "null",
        "boolean"
      ]
    },
    "name": {
      "selected": "true",
      "maxLength": 255,
      "inclusion": "available",
      "type": [
        "null",
        "string"
      ]
    },
    "id": {
      "selected": "true",
      "minimum": -2147483648,
      "inclusion": "automatic",
      "maximum": 2147483647,
      "type": [
        "null",
        "integer"
      ]
    }
  },
  "type": "object"
}
```

### Sync Mode

With an annotated `catalog.json`, the tap can be invoked in sync mode:

```
$ tap-toggl --config config.json --catalog catalog.json
```

Messages are written to standard output following the Singer specification. The resultant stream of JSON data can be consumed by a Singer target.


## Replication Methods and State File

### Incremental

The streams that are incremental are:

- workspaces
- clients
- groups
- projects
- tasks
- users
- workspace users
- time entries*

Time entries uses a lookback window set by the config's "detailed_report_trailing_days" to pull data, then uses replication key `updated` as the bookmark.

### Full Table

The only stream that is full table is `tags`.

## Tests

```
$ make test
```

Copyright &copy; 2018 Stitch
