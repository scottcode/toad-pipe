create table ppl_job
as
select people.name as people_name
    , people.age
    , job.job
from people
inner join
    job
on people.name = job.name
;