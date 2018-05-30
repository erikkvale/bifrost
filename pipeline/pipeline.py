class Pipeline:
    """
    This class is a base implementation of a data pipeline
    with linear dependencies. It is meant to assist in running
    a sequence of functions in a linear and explicit manner,
    allowing the user of the class to decorate their target
    functions and specify any dependent functions in a more
    dynamic way. See examples below.

    Notes
    -----
    This is meant to mimic function composition based on ordering
    to minimize side effects, so the functions should be designed
    and understood in terms of linear execution prior to using this class.

    Examples
    --------
    # Create a new instance of the Pipeline class
    pipeline = Pipeline()

    # Decorate a group of functions using the instance's task() method,
    # Note the dependency parameter specified after the first task

    @pipeline.task()
    def first_task(x):
        return x + 1

    @pipeline.task(depends_on=first_task)
    def second_task(x):
        return x * 2

    @pipeline.task(depends_on=second_task)
    def last_task(x):
        return x - 4

    # Check the tasks instance variable to see tasks
    # were added and ordered properly
    print(pipeline.tasks)
        [<function __main__.first_task>,
        <function __main__.second_task>,
        <function __main__.last_task>]

    # Then run the tasks
    pipeline_output = pipeline.run(20)

    # Make assertion for equality based on alternate method
    assert last_task(second_task(first_task(20))) == pipeline.run(20)
    True

    """

    def __init__(self):
        self.tasks = []

    def task(self, depends_on=None):
        """
        Instance method, intended to be used as a function
        decorator to update instance's tasks list, and maintain
        execution ordering of decorated functions using depends
        argument.

        Parameters
        ----------
        depends_on : function
            The actual function (functions are first-class objects)
            that the currently decorated task depends on.

        Returns
        -------
        function
        """

        idx = 0
        if depends_on:
            idx = self.tasks.index(depends_on) + 1

        def inner(f):
            self.tasks.insert(idx, f)
            return f

        return inner

    def run(self, _input):
        """
        The task runner method, function composition
        behavior.

        Parameters
        ----------
        _input : object
            This is the initial input to kick off the pipeline execution,
            i.e. the input for the root(first) task to be run in the pipeline.

        Returns
        -------
        object
        """
        output = _input
        for task in self.tasks:
            output = task(output)
        return output