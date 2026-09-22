# As a service

Definitions for OpenRC and systemd are in
[`packaging/`](https://github.com/QaidVoid/errand/tree/main/packaging). Both run
the binary as an ordinary account, not as root: the configuration file holds the
bot token, and every agent is started as that same account, so files an agent
writes in a project belong to the person whose project it is.

## What to prepare

```sh
useradd --system --home-dir /var/lib/errand --create-home errand

git clone https://github.com/QaidVoid/errand
cd errand && cargo build --release
install -m 0755 dist/errand /usr/local/bin/errand

install -o errand -g errand -m 0700 -d /var/lib/errand/.config/errand
install -o errand -g errand -m 0600 config.json \
        /var/lib/errand/.config/errand/config.json
```

## OpenRC

```sh
install -m 0755 packaging/errand.initd /etc/init.d/errand
install -m 0644 packaging/errand.confd /etc/conf.d/errand
$EDITOR /etc/conf.d/errand
rc-update add errand default
rc-service errand start
```

## systemd

```sh
install -m 0644 packaging/errand.service /etc/systemd/system/errand.service
$EDITOR /etc/systemd/system/errand.service
systemctl daemon-reload
systemctl enable --now errand
```

## Restarting, and not restarting

Both definitions restart a crash and refuse to restart a refusal. Exit 2, 3, and
4 are decisions the daemon made, and repeating them would only log the same line
again; 4 would also mean fighting the daemon that is already serving. See
[getting started](/start) for what each code means.

## Reloading without restarting

Send SIGHUP after editing the configuration file and the daemon re-reads it
without killing any session:

```sh
systemctl reload errand
```

A file that fails validation keeps the running configuration, and the refusal
is logged naming every problem. A valid file swaps what new sessions start
from and is carried to every live session, which logs what changed and when
each change lands:

- **Live.** Read on every use, so running sessions pick it up at once.
  Storage budgets lead here: raising `sandbox.disk` applies at the next disk
  check, with no sandbox touch. Timeouts, output shaping, chat membership,
  and the shutdown list are live too.
- **Next launch.** Written into each fresh sandbox policy, so new sessions
  and sandbox restarts pick it up while running sandboxes keep theirs.
  Scratch and single file sizes live here: a tmpfs size is fixed at mount
  and an rlimit at exec, so neither can move under a running agent. Raise
  `sandbox.tmpSize` and the session that is full now still needs its one
  automatic restart onto a fresh sandbox; the session after that starts
  large.
- **Restart.** Baked into objects built once at startup: the backend, the
  image, the network and broker shape, the directories, admission caps, and
  the chat connection. These are reported, not applied, until the daemon
  restarts.

## Limits

Both apply memory, cpu, and process limits to the daemon and everything it
starts, as one tree. That means one busy session can spend the whole budget.

Limiting each session separately needs a cgroup the sandbox may create children
in, named by `BAILEY_CGROUP_ROOT`, which the daemon passes through. Under
systemd that is the service's own delegated cgroup (`Delegate=yes` with
`DelegateSubgroup=supervisor`). Under OpenRC it has to be made and delegated by
hand, because OpenRC puts the daemon directly in the service cgroup and a cgroup
cannot both hold processes and hand its controllers to children.

Without one, the daemon says so at startup rather than implying a limit it is
not applying.
