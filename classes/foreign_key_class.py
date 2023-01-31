
class ForeignKey:
    def __init__(self, model, ref_cols, custom_resource=None):
        self.model = model
        self.natural_keys = ref_cols
        self.custom_resource = custom_resource
