#!/bin/bash

function die() {
    echo "$*"
    exit 1
}

# thanks to Christian Kaltepoth for the
# nice script
# http://chkal.blogspot.com/2010/09/maven-repositories-on-github.html

for DIR in $(find ./repository -type d); do
    (
        echo -e "<html>\n<body>\n<h1>Directory listing</h1>\n<hr/>\n<pre>"
        ls -1pa "${DIR}" | grep -v "^\./$" | grep -v "^index\.html$" | awk '{ printf "<a href=\"%s\">%s</a>\n",$1,$1 }'
        echo -e "</pre>\n</body>\n</html>"
    ) > "${DIR}/index.html"
    echo "Created ${DIR}/index.html"
done

## args are absolute path to docs jar and relative path to docs dir
function unpack() {
    DOCJAR="$1"
    DOCDIR="$2"
    BASEDOCJAR=$(basename "$DOCJAR")
    echo "Unpacking docs from $BASEDOCJAR to $DOCDIR"
    test -d "$DOCDIR" && /bin/rm -rf "$DOCDIR"
    mkdir -p "$DOCDIR"
    (cd "$DOCDIR" && jar xf "$DOCJAR")
}

for JD_JAR in $(cd repository && find . -name "*-javadoc.jar"); do
    JAR_ABSOLUTE=$(echo `pwd`"/repository/$JD_JAR")
    test -e "$JAR_ABSOLUTE" || die "Bug in script, not finding $JAR_ABSOLUTE"
    D=$(dirname "$JD_JAR")
    PROJ=$(basename $(echo "$D" | sed -e 's/_.*//g'))
    VERSION=$(basename "$D")

    unpack "$JAR_ABSOLUTE" api/"$PROJ"/"$VERSION"
    unpack "$JAR_ABSOLUTE" api/"$PROJ"/latest
done
