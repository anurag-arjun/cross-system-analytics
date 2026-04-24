---
title: Nexus Analytics Dashboard
---

# Nexus Analytics

Real-time cross-chain behavioral analytics.

## Event Overview

```js
const events = FileAttachment("data/events.json").json();
```

```js
const eventTypes = events.reduce((acc, ev) => {
  acc[ev.event_type] = (acc[ev.event_type] || 0) + 1;
  return acc;
}, {});
```

```js
Inputs.table(Object.entries(eventTypes).map(([type, count]) => ({type, count})))
```

## Recent Activity

```js
const recentEvents = events.slice(0, 20);
```

```js
Inputs.table(recentEvents)
```
