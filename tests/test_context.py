import os
from helpers import *


def test_context_bootstrap_plugins():
    from giblets import Component, implements
    from melkman.context import Context, IRunDuringBootstrap


    class FooComponent(Component):
        implements(IRunDuringBootstrap)

        did_bootstrap = 0
        
        def bootstrap(self, context, purge=False):
            FooComponent.did_bootstrap += 1

    config = {
        'plugins': [{'pattern': "test_context.FooComponent", "enabled": True}]
    }
    ctx = Context.from_dict(config, defaults=Context.from_yaml(test_yaml_file()).config)
    
    ctx.bootstrap(purge=False)
    
    assert FooComponent.did_bootstrap == 1
    
def test_context_components():

    from giblets import Component, implements, ExtensionInterface, ExtensionPoint
    from melkman.context import Context, IContextConfigurable

    class IBaz(ExtensionInterface):
        pass

    class FooComponent(Component):
        implements(IContextConfigurable, IBaz)

        def set_context(self, context):
            self.context = context
            
    class BarComponent(Component):
        implements(IContextConfigurable, IBaz)

        def set_context(self, context):
            self.context = context

    class BazHolder(Component):
        bazoo = ExtensionPoint(IBaz)



    config = """
        {
        "plugins": [
            {"pattern": "test_context.FooComponent", "enabled": true},
            {"pattern": "test_context.BarComponent", "enabled": false},
            {"pattern": "test_context.BazHolder", "enabled": true}]
        }
    """
    
    ctx = Context.from_json(config)
    
    bazer = BazHolder(ctx.component_manager)
    
    # check that only the enabled components came through
    assert len(bazer.bazoo) == 1
    assert isinstance(bazer.bazoo[0], FooComponent)

    # check that the component was configured with the context
    assert bazer.bazoo[0].context == ctx