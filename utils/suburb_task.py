


class SuburbTask:
    def __init__(self, name):
        self.name = name
        self.urls = None
        self.details = {}
        self.error = None

    def __str__(self):
        return str(self.name)

    def tag_details(self, tag, value):
        self.details[tag] = value


class SuburbPropertyTask(SuburbTask):
    def __init__(self, property_name):
        super().__init__(self.name)
        self.property = property_name
        
        self.property_brief = {}
        self.property_details = {}
         
        
