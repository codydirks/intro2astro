from itertools import groupby,chain
import requests
import random
import copy

# Returns all raw student info from a given Canvas course.
def _get_student_info(course_id, canvas_auth_token):
    canvas_domain='https://canvas.northwestern.edu'
    # Need to check number of students, since if >100 Canvas will paginate results,
    # which necessitates generating multiple queries
    max_per_page=100 # This seems to be the absolute max allowed, probably don't change this
    url=[canvas_domain,
         '/api/v1/courses/',
         str(course_id),
         '?include[]=total_students&access_token=',
         canvas_auth_token
        ]
    c=requests.get(''.join(url)).json()
    num_students=c['total_students']
    pages=int(num_students/max_per_page)+1

    # Grabs the pages of enrolled users from Canvas, returns all enrolled users
    base_student_url=[canvas_domain,
                      '/api/v1/courses/',
                      str(course_id),
                      '/enrollments',# Specifies course and desired data
                      '?type=StudentEnrollment', # Only grab students, ignore TAs, teachers
                      '&include[]=group_ids', # Also include group info - may be useful
                      '&per_page='+str(max_per_page),
                      '&access_token='+canvas_auth_token # parameters
                      ]

    student_pages=[requests.get(''.join(base_student_url)+'&page='+str(i)).json() for i in range(1,pages+1)]
    return list(chain.from_iterable(student_pages))



# This function returns student information from Canvas, given a course ID and an
# authentication token. Info is returned as a list of tuples with the format (name, group number)
def load_students_from_canvas_course(course_id, canvas_auth_token):
    stdnts=_get_student_info(course_id, canvas_auth_token)
    offset=min([stdnt['user']['group_ids'][0] for stdnt in stdnts])-1
    return [(stdnt['user']['sortable_name'], int(stdnt['user']['group_ids'][0]-offset)) for stdnt in stdnts]

# As an alternative to Canvas, can provide a csv file of student group info
# Each line of this file should have the following format:
# Last name, First name, Group number
def load_students_from_file(filename):
    stdnts=[]
    with open(filename, 'r') as myfile:
        for line in myfile:
            dat=[x.strip() for x in line.strip('\n').split(',')]
            name=dat[0]+', '+dat[1]
            stdnts.append((unicode(name, 'utf-8'),int(dat[2])))
    return stdnts



# This function takes a "results" list that is output from the peer-review
# assigning algorithm and emails the students their peer-review assignments.
def email_students_in_canvas(results, course_id, canvas_auth_token):
    canvas_domain='https://canvas.northwestern.edu'
    # Emailing students requires their user_id within Canvas, so we grab
    # student info so we can match names to ids
    student_info=_get_student_info(course_id, canvas_auth_token)

    # Construct the template email to be sent out
    email_subj='Peer Review Videos'
    email_body=''.join(['Hi {0},\n\n',
         'Please peer-review the videos from Groups {1} and {2}.\n\n',
         'Cheers,\n\n',
         '[YourNameHere]'])

    for result in results:
        user_id=[s['user']['id'] for s in student_info if s['user']['sortable_name']==result[0]][0]
        msg=''.join(email_body).format(result[0].split(',')[1].strip(), *result[2])
        email_url=[canvas_domain,
                  '/api/v1/conversations',
                  '?recipients[]='+str(user_id),
                  '&subject='+email_subj,
                  '&body='+msg,
                  '&access_token='+canvas_auth_token]
        requests.post(''.join(email_url))

# Canvas authorization token - should be generated by user on Canvas and copy-pasted here
canvas_auth_token=''

#Canvas course id number for this class
course_id=0

filename='test_file.csv'

#Choose how you load student info by commenting out either of these
students=load_students_from_canvas_course(course_id, canvas_auth_token)
#students=load_students_from_file(filename)


students.sort(key=lambda x: x[0])# Sort student list by name
group_ids=list(set([x[1] for x in students])) # Grabs set of all possible group ids
num_groups=len(group_ids)
reviews_per_student=2

# Generate the entire pool of reviews at once, to guarantee each video is in the pool
# a minimum number of times. Some groups will get one extra review, depending on
# the ratio of total reviews to the number of groups.
initial_pool=[group_ids[ip%num_groups] for ip in range(reviews_per_student*len(students))]

# Initialize the results array, where each element has the format:
# (name, group, [list of videos to review])
initial_results=[(stdnt[0], stdnt[1], reviews_per_student*[None])for stdnt in students]

success=False
while success==False:
    success=True
    pool=copy.copy(initial_pool)
    results=copy.copy(initial_results)
    for result in results:
        if success:
            for i in range(reviews_per_student):
                possible_choices=list(set([p for p in pool if (p!= result[1] and p not in result[2])]))
                if len(possible_choices)==0:
                    # We've encountered a state where a student can't be assigned enough reviews,
                    # so we should start over.
                    success=False
                    break
                else:
                    choice=random.choice(possible_choices)
                    pool.remove(choice)
                    result[2][i]=choice
        else:
            break

[a[2].sort() for a in results]
# If a student was somehow assigned their own group, or assigned duplicate reviews,
# that will be printed here
for result in results:
    if (result[1] in result[2]) or len(result[2]) != len(list(set(result[2]))):
        print result
all_vids=list(chain(*[b[2] for b in results]))
counts=[all_vids.count(item) for item in set(all_vids)]
final_counts=[(idx,counts.count(idx)) for idx in set(counts)]
final_results=copy.deepcopy(results)




# If you used Canvas to get student info, you can also use Canvas to send out emails informing the students which videos they are to peer-review. The command to do so is commented out below to avoid accidentally sending out a mass email.

#Before running this command, please double-check the function's code (defined along with the other functions in the 2nd cell of this notebook), and verify that the Canvas domain is correct, and that the email subject and body are to your liking. Once you've done so, you can uncomment and run the function below.

#email_students_in_canvas(final_results, course_id, canvas_auth_token)
