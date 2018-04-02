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





# Disclaimer

This project was done in its entirety by Eric Vaitl and Ankita
Joshi. We hereby state that we have not received unauthorized help of
any form.

