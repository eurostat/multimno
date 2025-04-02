import pandas as pd
import json


def load_sbom():
    # Load the JSON file
    with open('/opt/app/license/sbom.json', 'r') as file:
        data = json.load(file)

    # Extract relevant fields
    components = data['components']
    extracted_data = []

    for component in components:
        name = component.get('name', None)
        version = component.get('version', None)
        
        # Iterate over each license in the licenses list
        for license_info in component.get('licenses', []):
            license_id = license_info.get('license', {}).get('id', None)
            license_name = license_info.get('license', {}).get('name', None)
            license_content = license_info.get('license', {}).get('text', {}).get('content', None)
            
            extracted_data.append({
                'name': name,
                'version': version,
                'license_id': license_id,
                'license_name': license_name,
                'license_content': license_content
            })

    # Load into pandas DataFrame
    sbom_df = pd.DataFrame(extracted_data)

    return sbom_df


def obtain_license_df(sbom_df):
    # License df accumulator
    dfs = []

    df_license_id, temp_df = extract_by_license_id(sbom_df)
    dfs.append(df_license_id)

    df_license_name, temp_df = extract_by_license_name(temp_df)
    dfs.append(df_license_name)

    df_license_content, temp_df = extract_by_license_content(temp_df)
    dfs.append(df_license_content)

    # Concatenate all dfs
    df = pd.concat(dfs).sort_values(by='name')

    return df

def extract_by_license_id(df):
    # Extract rows with license_id
    df_license_id = df[df['license_id'].notnull()]
    # Drop duplicates based on name
    df_license_id = df_license_id.drop_duplicates(subset=['name'])
    # Select relevant columns
    df_license_id = df_license_id.loc[:, ['name', 'version', 'license_id']].rename(columns={'license_id': 'license'})
    # Remove dependencies that have license_id from original df (left anti join)
    temp_df = df[~df['name'].isin(df_license_id['name'])]

    return df_license_id, temp_df

def extract_by_license_name(df):
    df_license_name = df[df['license_name'].notnull()]
    df_license_name = df_license_name[df_license_name['license_name'].str.contains(r'::')]
    df_license_name['license_id'] = df_license_name['license_name'].str.extract(r'([^::]+)$')

    df_license_name = df_license_name.drop_duplicates(subset=['name'])
    df_license_name = df_license_name.loc[:, ['name', 'version', 'license_id']].rename(columns={'license_id': 'license'})

    temp_df = df[~df['name'].isin(df_license_name['name'])]

    return df_license_name, temp_df

def extract_by_license_content(df):
    df_license_content = df[df['license_content'].notnull()]
    df_license_content = df_license_content.drop_duplicates(subset=['name'])
    df_license_content = df_license_content.loc[:, ['name', 'version', 'license_content']].rename(columns={'license_content': 'license'})

    temp_df = df[~df['name'].isin(df_license_content['name'])]

    return df_license_content, temp_df


def main():
    print('Starting license extraction...')

    # Load the SBOM
    print('Loading SBOM...')
    sbom_df = load_sbom()

    # Obtain the license DataFrame
    print('Obtaining license DataFrame...')
    license_df = obtain_license_df(sbom_df)

    # Save the license DataFrame
    print('Saving license DataFrame...')
    license_df.to_csv('/opt/app/license/licenses.csv', index=False)

    # Save the unique licenses to a text file
    print('Saving unique licenses...')
    unique_licenses = sorted(set(license_df['license']))
    with open('/opt/app/license/unique_licenses.txt', 'w') as file:
        for license in unique_licenses:
            file.write(license + '\n')

    print('License extraction complete!')

if __name__ == '__main__':
    main()