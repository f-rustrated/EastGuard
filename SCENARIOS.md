1. Each message scenarios 
- CreateTopic -> check whether topic is created (blocker ListTopic) 
- DeleteTopic -> check whether topic is deleted (blocker ListTopic)
- ListTopic -> list created topics 
- DescribeTopic -> describe topic 

- Produce / Fetch -> check whether messages are really produced 

- DescribeCluster -> describe cluster so that informative data is included 
- ListTopicsWithStats -> ... 
- SplitRange -> check whether the range is splitted correctly 







## Update docs 

- Naming consistency 
  - SegmentRingBuffer ? Segment Cache ?
  - write_cursor ? tail ? 
  - read_cursor ? commit_offset ? 
- What happens to existing ConnectionRequests with variants Connection, Query, Propose? 
- 