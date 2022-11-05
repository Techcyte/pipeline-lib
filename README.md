
Online basic data transformations:

* Group by
    * Input: Iterator of sequentially similar items (i.e. items with same sample_id)
    * Output: 
        - group key(s)
        - other key: [other item]
    * Constants:
        - id: key to group values on (i.e `region_id`)
        - limit: Union[int, None], max number of items of same id to grou

* Splitter
    * Input:    
        - key1: type1
        - key2: list[type2]
    * Output:
        - key1: type1
        - key2: type2
    * Constants:
        - split_keys: [key1, key2, key3]    
        - 
    
* Group key
* Additional outputs
    * Logger
    * Terminate
    * Finished sample


    
