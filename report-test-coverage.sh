#!/bin/bash

## THIS IS A HACK
#
# This is a quick hack to get coverage reports using mocha and jscoverage
# there's probably a much better way to do this, but this is ok for now
# You can grab jscoverage for node at:
# https://github.com/visionmedia/node-jscoverage
# based on JSCoverage from: http://siliconforks.com/

TMPDIR=`mktemp -d /tmp/nbg-coverage.XXXXXXX `
OURDIR=`pwd`

jscoverage --exclude=node_modules --exclude=.git --no-instrument=test $OURDIR $TMPDIR
sed -i -- 's/--reporter list/--reporter html-cov/' $TMPDIR/test/mocha.opts
(cd $TMPDIR &&  npm install && npm test > $OURDIR/test-coverage.html)
