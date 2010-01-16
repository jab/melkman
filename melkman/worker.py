from giblets import ExtensionInterface


class IWorkerProcess(ExtensionInterface):
    """
    implement this interface to provide ongoing background processes to 
    the backend.
    """
    
    def run(context):
        """
        run the background process in the context given.  
        This function should not return unless the background process is complete.
        """
    