Examples in Lisp (:cl-czmq)

- You need:
  - [ZMQ](zeromq.org) (of course)
  - [CZMQ](http://czmq.zeromq.org/)
  - [cl-czmq](https://github.com/lhope/cl-czmq)
  - [sbcl](http//sbcl.org) (other lisps are untested).
  - [quicklisp](http://quicklisp.org)
- Ensure quicklisp is loaded in your .sbclrc.
- Link or download cl-czmq into your `quicklisp/local-projects/` directory
- Run example.lisp with `./run example.lisp`

Note the examples are slightly unidiomatic (particularly the
loops). This is to make them closer to the original C examples.

See LICENSE in examples directory
