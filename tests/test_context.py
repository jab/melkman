import os
from helpers import *



def test_context_from_ini():
    from melkman.context import Context
    ctx = Context.from_ini(os.path.join(data_path(), "test_context_from_ini.ini"))
    
    assert ctx.config['a.b.c'].foo == 'bar'
    assert ctx.config.test.foo.bar.quux == 'flup'
    
def test_context_bootstrap_plugins():
    from giblets import Component, implements
    from melkman.context import Context, IRunDuringBootstrap


    class FooComponent(Component):
        implements(IRunDuringBootstrap)

        did_bootstrap = 0
        
        def bootstrap(self, context, purge=False):
            FooComponent.did_bootstrap += 1

    config = {"test_context.FooComponent": {"plugin_enabled": True}}
    ctx = Context.from_dict(config, defaults=Context.from_ini(test_ini_file()).config)
    
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
        "test_context.FooComponent": {"enabled": true},
        "test_context.BarComponent": {"enabled": false},
        "test_context.BazHolder": {"enabled": true},
        "unrelated.OtherJunk": {"abc": 123}
        }
    """
    
    ctx = Context.from_json(config)
    
    bazer = BazHolder(ctx.component_manager)
    
    # check that only the enabled components came through
    assert len(bazer.bazoo) == 1
    assert isinstance(bazer.bazoo[0], FooComponent)

    # check that the component was configured with the context
    assert bazer.bazoo[0].context == ctx