========
liveview
========


Real time browser <> ASGI communication / re-rendering for Python (inspired by Phoenix Live View)

.. warning:: **Experimental Software**


Description
===========

Erlang, Elixir and Phoenix are awesome… but sometimes we just need something
that is simpler and that must run on Python.

This package provides a simple implementation for something that resembles `Phoenix LiveView`_,
but less ambitious and much more unstable.
The main idea is to be able to drive changes in the browser using a Python
`ASGI`_ server.

Example Use Cases
-----------------

- **Live editing and responsive preview** for
  - SVG
  - markdown
  - reStructuredText (*and other markup languages …*)
  - plantUML
  - nwDiag
  - graphviz (*and other text-driven diagram tools …*)

- **matplotlib graphs** and other kinds of server generated content

- **Avoid writing JS** for simple applications


Note
====

This project has been set up using PyScaffold 3.2.3. For details and usage
information on PyScaffold see https://pyscaffold.org/.


.. _Phoenix LiveView: https://hexdocs.pm/phoenix_live_view/Phoenix.LiveView.html#content
.. _ASGI: https://asgi.readthedocs.io/en/latest/
