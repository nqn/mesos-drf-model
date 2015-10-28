# Events

Events are written to `events.txt` and contains newline separated json objects representing events from the simulated runs.

```
{
  "name": "offer_issued",
  "source": "allocator",
  "id": "drf_allocator",
  "data": "Offer for resources [4, 3] issued to framework A",
  "time": 132
}
{
  "name": "offer_declined",
  "source": "framework",
  "id": "A",
  "data": "",
  "time": 133
}
{
  "name": "allocator_status",
  "source": "allocator",
  "id": "drf_allocator",
  "data": {"user_shares": {"A": 0.3333, "B": 0.66667}},
  "time": 133
}
```
