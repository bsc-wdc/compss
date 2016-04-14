# This script removes Python comments
BEGIN { start = 0; incomment = 0;}

{
    if (start == 0)
    {
            # Python comment
            if (/^\#/)
            {
                # print "single line"
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
        # print the complete line for the rest of the file
        print $0
    }
}
