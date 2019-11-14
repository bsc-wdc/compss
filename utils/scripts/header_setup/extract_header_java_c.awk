BEGIN { incomment = 0; }
{
    # Search multi-line/block comment begin
    if (incomment == 0)
    {
        # C++ single line comment
        if (/^\/\//)
        {
            # print "single line"
            print $0;
        }
        # single line comment
        else if (/^\/\*[^\/]*\*\/$/)
        {
            # print "single line"
            print $0;
        }
        # multi line comment
        else if (/^\/\*/)
        {
            #print "start comment"
            # start multi line comment
            print $0;
            incomment = 1
        }
        else
        {
            # first line that is not a comment, start normal output
            print $0;
            start = 1;
        }
    }
    else
    {
        # search for comment end, and then exit once found
        if (/\*\//)
        {
            print $0;
            exit
        }
        else
        {
            print $0
        }
    }
}
