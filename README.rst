====================================
 DynamoDB Done Count Design Pattern
====================================

We have a use-case which is probably pretty common: a lot of async
processes (Lambdas) complete work in parallel, and we need to know
when they're all done. We're looking for an efficient way to determine
this. We believe we can use DynamoDB's conditional updates to track
completed tasks, tallying a count, and stopping when the count matches
the number of async tasks.

More concretely, we're processing pages of a document in parallel, so
we know the number of pages -- say, 500. When each Lambda finishes a
page, it increments a count in Lambda, but only if the page isn't
already in the list of completed pages. We need this because Lambda
may launch more than once on a given event (page stored to S3); it's
an edge case, but in volume, we've seen this about 0.1% of the time.

In the pattern we use in this demo, the pages are kept in a Set of
numbers. We check that the page number doesn't already exists, and add
our page number to the set. DynamoDB gives us back, for free, a
*consistent* collection of the new data. This means our submitting
Lambda gets back the updated count, and it can launch another Lambda
to (in our case) consolidate all the pages to complete the job. The
consistency and return are free in the DynamoDB billing model IIRC, so
it's a win.

DynamoDB bills by read and write "capacity units", where an WCU is 1KB
and an eventually-consistent RCU is 4KB. As we add more pages to the
set of done page numbers, we see the WCU increase from the minimum of
1, up to 3 for 1000 pages, and 6 for 2000 pages in the set. I don't
see a way to prevent the growing set from incurring a WCU cost, and
can't come up with an alternate model which separates this cost-event
from the done check.

I'm also a little concerned about a herd of Lambdas causing a spike in
WCU requirements. Typically, we would have to provision a high WCU
limit to accommodate these spikes. As of November 2018's re:Invent
announcement, we can now use on-demand pricing which imposes no limit
(it is about 6.5x more expensive than pre-provisioned limits, but will
be more cost-efficient for spikey loads like this).
