# Introduction

This is a solution to CSCI 6780
[Programming Project 4](./docs/Programming-Project4.pdf).

The repo for this project is at https://github.com/evaitl/6780p4.

# Running

Type "make" to build.

Create a bootstrap config file and as many ns config files as you
would like to run.  Run the bootstap server with "java BCH <cfg name>".
Run the other servers with "java CH <cfg name>".

Each CH will join the ring after it gets an `enter` command on its
input. It will leave the ring when it gets an `exit` command.


# Protocol

The various name servers need a protocol to talk to one another. This
is it. 

Each NS/BNS will keep track of next/prev 
ipaddr/port in the ring. 

- query

Response "rangeLower rangeUpper nextAddr nextPort". The BNS will
always send 1024 as rangeUpper.

- enterprev nextAddr nport

Sent to previous NS when entering the ring. Response "ok". The
entering NS is responsible for entering at the correct location in the
ring.

- enternext id addr port

Sent to next NS when entering the ring. Response is "ok" followed by
zero or more number "insert" messages from upstream as a transfer of
data. The new range for the receiver starts with id. 

- exitnext lowerId prevAddr prevPort

Sent to next NS when exiting. Response is "ok". Next NS range now
starts at lowerId. If the BNS gets this and lowerId is 0, then the BNS
range is (-1...1023] instead of (lowerId..1023]

- exitprev upperAddr upperPort

Sent to previous NS when exiting. Response is "ok". 

- lookup key

return "ok msg", "no addr port", or "na" . On "no", addr/port are for
next server in ring. "na" means this is the right NS, but no data.

- insert key msg

return "ok" or "no nextAddr nextPort".

- delete key

return "ok" or "na", or "no nextAddr nextPort". On "no", addr/port are
for next server in ring. "na" means this is the right NS, but no data.

Messages and responses are in a single line terminated with '\n'. One
message and response pair per TCP connection, which is then torn down.

# Disclaimer

This project was done in its entirety by Eric Vaitl and Ankita
Joshi. We hereby state that we have not received unauthorized help of
any form.

