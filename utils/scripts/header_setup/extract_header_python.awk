BEGIN { incomment = 0; }
{
    if (incomment == 0)
    {
        # Search multi-line/block comment begin
        if (/^[#]/)
        {
            incomment = 1;
        }
        print $0
    }
    else
    {
        # search for comment end, end then quit
        if (/^[^#]|^$/)
        {
            exit
        }
        print $0;
    }
}
