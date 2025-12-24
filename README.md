# Tile generator

This is a very simple tile generator and a small tile server written from scratch.

It parses OSM pbf, creates and simplifies geometry(along with extra metadata) and stores
it in SQLite DB.

At this momene, it doesn't follow any best practices and doesn't use any known tile formats.

The goal is learn how to build tile generators and support the [main map project](https://github.com/ShashlikMap/shashlik-map) as a data source.
