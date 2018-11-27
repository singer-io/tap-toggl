
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

Incremental replication works in conjunction with a state file to only extract new records each time the tap is invoked.

## Tests

```
$ make test
```

## License (MIT)

```
WWWWWW||WWWWWW
 W W W||W W W
      ||
    ( OO )__________
     /  |           \
    /o o|    MIT     \
    \___/||_||__||_|| *
         || ||  || ||
        _||_|| _||_||
       (__|__|(__|__|
```

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the 'Software'), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.