import re
import hashlib

FINGERPRINT_PREFIX = 'NVDCVE'

class CVE:
    def __init__(self, cve=None):
        self.cve = cve

    @staticmethod
    def product(cpe23Uri):
        ''' Extracts product fields from CPE with ECS prefix '''
        cpe_re = re.compile('(?i)(?P<spec>cpe):(?P<spec_version>\d\.\d):(?P<platform>\w):(?P<vendor>.*?):(?P<product>.*?):(?P<product_version>.*?):(?P<product_update>.*?):(?P<product_edition>.*?):(?P<product_language>.*?):(?P<sw_edition>.*?):(?P<target_sw>.*?):(?P<target_hw>.*?):(?P<other>.*?)$')
        match_cpe = re.match(cpe_re, cpe23Uri)
        transform_fields = {f'impacted.{key}': value for key, value in match_cpe.groupdict().items()}
        return transform_fields

    @staticmethod
    def fingerprint(original_date, unique_value):
        ''' Generates a sorteable document ID for ES'''
        hex_date = str(original_date).encode('utf-8').hex()
        hash = hashlib.blake2b(f'{FINGERPRINT_PREFIX}{unique_value}'.encode('utf-8'),
            digest_size=20).hexdigest()
        return  f'{hex_date}{hash}'

    @property
    def references(self):
        ''' Denormalizes CVE references with ECS schema '''
        return [{
            'event.provider': 'NVD',
            'nvd.dataset': 'CVE',
            'nvd.module': 'references',
            'vulnerability.id': self.cve['cve']['CVE_data_meta']['ID'],
            'vulnerability.type': 'CVE',
            'publish_date': self.cve['publishedDate'],
            'modified_date': self.cve['lastModifiedDate'],
            'reference.url': reference['url'],
            'reference.name': reference['name'],
            'reference.source': reference['refsource'],
            'reference.tags': reference.get('tags', []),
            'tags': [self.cve['cve']['CVE_data_meta']['ID'].lower()],
            'fingerprint': CVE.fingerprint(self.cve['publishedDate'],
                f'{self.cve["cve"]["CVE_data_meta"]["ID"]}{reference["name"]}')
        } for reference in self.cve['cve']['references']['reference_data']]

    @property
    def cvss2(self):
        ''' Denormalizes CVE CVSSv2 with ECS schema '''
        return {
            'cvss2.vector': self.cve['impact']['baseMetricV2']['cvssV2']['vectorString'],
            'cvss2.access_vector': self.cve['impact']['baseMetricV2']['cvssV2'].get('accessVector'),
            'cvss2.authentication': self.cve['impact']['baseMetricV2']['cvssV2'].get('authentication'),
            'cvss2.base_score': self.cve['impact']['baseMetricV2']['cvssV2'].get('baseScore'),
            'cvss2.integrity_impact': self.cve['impact']['baseMetricV2']['cvssV2'].get('integrityImpact'),
            'cvss2.availability_impact': self.cve['impact']['baseMetricV2']['cvssV2'].get('availabilityImpact'),
            'cvss2.confidentiality_impact': self.cve['impact']['baseMetricV2']['cvssV2'].get('confidentialityImpact'),
            'cvss2.severity': self.cve['impact']['baseMetricV2']['severity'],
            'cvss2.exploitability_score': self.cve['impact']['baseMetricV2'].get('exploitabilityScore'),
            'cvss2.impact_score': self.cve['impact']['baseMetricV2'].get('impactScore'),
            'cvss2.obtain_all_privilege': self.cve['impact']['baseMetricV2'].get('obtainAllPrivilege'),
            'cvss2.obtain_user_privilege': self.cve['impact']['baseMetricV2'].get('obtainUserPrivilege'),
            'cvss2.obtain_other_privilege': self.cve['impact']['baseMetricV2'].get('obtainOtherPrivilege'),
            'cvss2.user_interaction_required': self.cve['impact']['baseMetricV2'].get('userInteractionRequired'),
        } if self.cve['impact'].get('baseMetricV2') else { }

    @property
    def cvss3(self):
        ''' Denormalizes CVE CVSSv3 with ECS schema '''
        return {
            'cvss3.vector': self.cve['impact']['baseMetricV3']['cvssV3']['vectorString'],
            'cvss3.attack_vector': self.cve['impact']['baseMetricV3']['cvssV3'].get('attackVector'),
            'cvss3.privileges_required': self.cve['impact']['baseMetricV3']['cvssV3'].get('privilegesRequired'),
            'cvss3.user_interaction': self.cve['impact']['baseMetricV3']['cvssV3'].get('userInteraction'),
            'cvss3.attack_complexity': self.cve['impact']['baseMetricV3']['cvssV3'].get('attackComplexity'),
            'cvss3.scope': self.cve['impact']['baseMetricV3']['cvssV3']['scope'],
            'cvss3.confidentiality_impact': self.cve['impact']['baseMetricV3']['cvssV3'].get('confidentialityImpact'),
            'cvss3.availability_impact': self.cve['impact']['baseMetricV3']['cvssV3'].get('availability_impact'),
            'cvss3.base_score': self.cve['impact']['baseMetricV3']['cvssV3'].get('baseScore'),
            'cvss3.base_severity': self.cve['impact']['baseMetricV3']['cvssV3'].get('baseSeverity'),
            'cvss3.exploitability_score': self.cve['impact']['baseMetricV3'].get('exploitabilityScore'),
            'cvss3.impact_score': self.cve['impact']['baseMetricV3'].get('impactScore')
        } if self.cve['impact'].get('baseMetricV3') else { }

    @property
    def impact(self):
        ''' Denormalizes CVE impact with ECS schema '''
        return {
            **self.cvss2,
            **self.cvss3
        } if self.cve['impact'] else { }

    @property
    def details(self):
        ''' Parses CVE details with ECS schema '''
        return {
            'event.provider': 'NVD',
            'nvd.dataset': 'CVE',
            'nvd.module': 'details',
            'vulnerability.id': self.cve['cve']['CVE_data_meta']['ID'],
            'vulnerability.type': 'CVE',
            'vulnerability.description': '\n'.join([desc['value'] for desc in self.cve['cve']['description']['description_data']]),
            'publish_date': self.cve['publishedDate'],
            'modified_date': self.cve['lastModifiedDate'],
            'nlp.products': self.cve['entities']['PRODUCT'], 
            'nlp.org': self.cve['entities']['ORG'],
            'nlp.person': self.cve['entities']['PERSON'],
            'nlp.polarity': self.cve['sentiment']['polarity'],
            'nlp.subjectivity': self.cve['sentiment']['subjectivity'],
            'nlp.assessments': self.cve['sentiment']['assessments'],
            'nlp.props': self.cve['props'],
            'tags': [self.cve['cve']['CVE_data_meta']['ID'].lower()] + self.cve['props'] + self.cve['entities']['PERSON'] + self.cve['entities']['ORG'] + self.cve['entities']['PRODUCT'],
            'fingerprint': CVE.fingerprint(self.cve['publishedDate'],
                self.cve['cve']['CVE_data_meta']['ID']),
            **self.impact
        }

    @property
    def impacted(self):
        ''' Denormalizes CVE impact with ECS schema '''
        nodes = []
        if self.cve['configurations']:
            for node in self.cve['configurations'].get('nodes', []):
                for match in node.get('cpe_match', []):
                    parsed_cpe = CVE.product(match['cpe23Uri'])
                    nodes.append({
                        'event.provider': 'NVD',
                        'nvd.dataset': 'CVE',
                        'nvd.module': 'impacted',
                        'tags': [
                            self.cve['cve']['CVE_data_meta']['ID'].lower(), 
                            parsed_cpe.get('impacted.vendor').lower(),
                            parsed_cpe.get('impacted.product').lower()
                        ],
                        'vulnerability.id': self.cve['cve']['CVE_data_meta']['ID'],
                        'vulnerability.type': 'CVE',
                        'modified_date': self.cve['lastModifiedDate'],
                        'publish_date': self.cve['publishedDate'],
                        'impacted.operator': node['operator'],
                        'impacted.vulnerable': match['vulnerable'],
                        'impacted.cpe': match['cpe23Uri'],
                        'impacted.version_end_including': match.get('versionEndIncluding'),
                        'impacted.version_end_excluding': match.get('versionEndExcluding'),
                        'fingerprint': CVE.fingerprint(self.cve['publishedDate'],
                            f'{self.cve["cve"]["CVE_data_meta"]["ID"]}{match["cpe23Uri"]}'),
                        **parsed_cpe,
                    })
        return nodes