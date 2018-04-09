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

Each CH/BCH will keep track of upstream/downstream
ipaddr/port. Upstream is closer to the BCH and has lower ranges.

- query

Response "rangeLower rangeUpper nextAddr nextPort"

- enterdown addr port

Sent to previous CH when entering the ring. Response "ok". 

- enterup id addr port

Sent to next CH when entering the ring. Response is "ok" followed by
zero or more number "insert" messages from upstream as a transfer of
data. assert id is in the range up the next CH. 

- exitup lowerId lowerAddr lowerPort

Sent to next CH when exiting. Response is "ok".

-exitdown upperAddr upperPort

Sent to previous CH when exiting. Response is "ok". 

- lookup key

return "ok msg", "no addr port", or "na" . On no, addr/port are for next
server in ring. "na" means this is the right CH, but no data. 

- insert key msg

return "ok" or "no addr port".

After an exitup/exitdown, the CH sends all of its data upstream with
insert commands.

- delete key

return "ok" or "no addr port"

Messages and responses are in a single line terminated with '\n'. One
message and response pair per TCP connection, which is then torn down.



# Disclaimer

This project was done in its entirety by Eric Vaitl and Ankita
Joshi. We hereby state that we have not received unauthorized help of
any form.

