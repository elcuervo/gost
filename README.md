# Gost

Gost is a port of Ost

## Connect to a Redis server

```go
gost := gost.Connect(":6379")
```

## Push ids to a queue

```go
gost.Push("my_jobs", "id_to_be_procesed")
```

## View the items in that given queue

```go
gost.Items("my_jobs")
```

## Stop all accesed queues

```go
gost.Stop()
```

## Consume the elements in the queue

```go
gost.Each("my_jobs", func(id string) bool {
        if(does_something_with_the_id(id)) {
                // Everything is ok
                return true
        } else {
                // If the fn returns false the items is kept in the backup key
                return false
        }
})
```

