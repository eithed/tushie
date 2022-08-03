<?php

namespace Mannum\Common\Concerns;

use Closure;
use Illuminate\Database\QueryException;
use Illuminate\Support\Str;
use Illuminate\Support\Facades\DB;

trait Tushie
{
    public function getRows()
    {
        return $this->rows;
    }

    public function getSchema()
    {
        return $this->schema ?? [];
    }

    protected function cacheReferencePath()
    {
        return (new \ReflectionClass(static::class))->getFileName();
    }

    protected function shouldCache()
    {
        return property_exists(static::class, 'rows');
    }

    public static function bootTushie()
    {
        $instance = (new static);

        $className = static::class;
        $dataPath = $instance->cacheReferencePath();

        if (!$instance->shouldCache() ||
            (($entry = $instance->findMetadata($className)) !== null ? 
            filemtime($dataPath) > $entry->data_path_mtime : true)) {
            $entry = $instance->refreshMetadata($className, filemtime($dataPath));

            $instance->migrate($entry);
        }
    }

    public function getTable()
    {
        return implode('_', ['tushie', md5(get_class($this))]);
    }

    protected function migrate()
    {
        $rows = $this->getRows();
        $tableName = $this->getTable();
        
        if (count($rows)) {
            $this->createTable($tableName, $rows[0]);
        } else {
            $this->createTableWithNoData($tableName);
        }

        static::truncate();

        foreach (array_chunk($rows, $this->getInsertChunkSize()) ?? [] as $inserts) {
            if (!empty($inserts)) {
                static::insert($inserts);
            }
        }
    }

    protected function createTable(string $tableName, $firstRow)
    {
        $this->createTableSafely($tableName, function ($table) use ($firstRow) {
            // Add the "id" column if it doesn't already exist in the rows.
            if ($this->incrementing && ! array_key_exists($this->primaryKey, $firstRow)) {
                $table->increments($this->primaryKey);
            }

            foreach ($firstRow as $column => $value) {
                switch (true) {
                    case is_numeric($value):
                        $type = 'float';
                        break;
                    case is_string($value):
                        $type = 'text';
                        break;
                    case is_object($value) && $value instanceof \DateTime:
                        $type = 'dateTime';
                        break;
                    default:
                        $type = 'string';
                }

                if ($column === $this->primaryKey && $type == 'integer') {
                    $table->increments($this->primaryKey);
                    continue;
                }

                $schema = $this->getSchema();

                $type = $schema[$column] ?? $type;

                $table->{$type}($column)->nullable();
            }

            if ($this->usesTimestamps() && (! in_array('updated_at', array_keys($firstRow)) || ! in_array('created_at', array_keys($firstRow)))) {
                $table->timestamps();
            }
        });
    }

    protected function createTableWithNoData(string $tableName)
    {
        $this->createTableSafely($tableName, function ($table) {
            $schema = $this->getSchema();

            if ($this->incrementing && ! in_array($this->primaryKey, array_keys($schema))) {
                $table->increments($this->primaryKey);
            }

            foreach ($schema as $name => $type) {
                if ($name === $this->primaryKey && $type == 'integer') {
                    $table->increments($this->primaryKey);
                    continue;
                }

                $table->{$type}($name)->nullable();
            }

            if ($this->usesTimestamps() && (! in_array('updated_at', array_keys($schema)) || ! in_array('created_at', array_keys($schema)))) {
                $table->timestamps();
            }
        });
    }

    protected function createTableSafely(string $tableName, Closure $callback)
    {
        $schemaBuilder = DB::getSchemaBuilder();

        try {
            $schemaBuilder->create($tableName, $callback);
        } catch (QueryException $e) {
            if (Str::contains($e->getMessage(), 'already exists (SQL: create table')) {
                // This error can happen in rare circumstances due to a race condition.
                // Concurrent requests may both see the necessary preconditions for
                // the table creation, but only one can actually succeed.
                return;
            }

            throw $e;
        }
    }

    public function usesTimestamps()
    {
        // Override the Laravel default value of $timestamps = true; Unless otherwise set.
        return (new \ReflectionClass($this))->getProperty('timestamps')->class === static::class
            ? parent::usesTimestamps()
            : false;
    }

    public function getInsertChunkSize() {
        return $this->insertChunkSize ?? 100;
    }

    public function refreshMetadata(string $className, int $dataPathMtime) : mixed
    {
        $this->createTableSafely('tushie_metadata', function ($table) {
            $table->string('class_name');
            $table->string('table_name');
            $table->integer('data_path_mtime');
        });


        return DB::table('tushie_metadata')->updateOrInsert([
            'class_name' => $className,
            'table_name' => $this->getTable(),
        ], [
            'data_path_mtime' => $dataPathMtime
        ]);
    }

    public function findMetadata(string $className) : mixed
    {
        try {
            return DB::table('tushie_metadata')->where([
                'class_name' => $className
            ])->first();
        } catch (QueryException $e) {
            if (Str::contains($e->getMessage(), 'does not exist')) {
                return null;
            }

            throw $e;
        }
    }
}
